use redis::{aio::ConnectionLike, Value};

use crate::{
    assignments::FromTable,
    result_set::{Column, FromRedisValueWithGraph, Scalar, Statistics, Take},
    server_type_error, RedisGraphError, RedisGraphResult, RedisString, ResultSet,
};

/// Represents a single graph in the database.
pub struct AsyncGraph<C> {
    conn: C,
    /// Information about the graph.
    graph_info: tokio::sync::Mutex<crate::graph::GraphInfo>,
}

unsafe impl<C: Send> Send for AsyncGraph<C> {}
unsafe impl<C: Sync> Sync for AsyncGraph<C> {}

impl<C> AsyncGraph<C>
where
    C: ConnectionLike + Clone,
{
    /// Opens the graph with the given name from the database.
    ///
    /// If the graph does not already exist, creates a new graph with the given name.
    pub async fn open(conn: C, name: String) -> RedisGraphResult<Self> {
        let graph = Self {
            conn,
            graph_info: tokio::sync::Mutex::new(crate::graph::GraphInfo {
                name,
                labels: Vec::new(),
                relationship_types: Vec::new(),
                property_keys: Vec::new(),
            }),
        };

        // Create a dummy node and delete it again.
        // This ensures that an empty graph is created and `delete()`
        // will succeed if the graph did not already exist.
        graph.mutate("CREATE (dummy:__DUMMY_LABEL__)").await?;
        graph
            .mutate("MATCH (dummy:__DUMMY_LABEL__) DELETE dummy")
            .await?;

        Ok(graph)
    }

    /// Executes the given query and returns its return values.
    ///
    /// Only use this for queries with a `RETURN` statement.
    pub async fn query<T: FromTable>(&self, query: &str) -> RedisGraphResult<T> {
        self.query_with_statistics(query)
            .await
            .map(|(value, _)| value)
    }

    /// Same as [`query`](#method.query), but also returns statistics about the query along with its return values.
    pub async fn query_with_statistics<T: FromTable>(
        &self,
        query: &str,
    ) -> RedisGraphResult<(T, Statistics)> {
        let response: Value = self.request(query).await?;
        let result_set = self.get_result_set(response).await?;
        let value = T::from_table(&result_set)?;
        Ok((value, result_set.statistics))
    }

    /// Executes the given query while not returning any values.
    ///
    /// If you want to mutate the graph and retrieve values from it
    /// using one query, use [`query`](#method.query) instead.
    pub async fn mutate(&self, query: &str) -> RedisGraphResult<()> {
        self.mutate_with_statistics(query).await.map(|_| ())
    }

    /// Same as [`mutate`](#method.mutate), but returns statistics about the query.
    pub async fn mutate_with_statistics(&self, query: &str) -> RedisGraphResult<Statistics> {
        let response: Value = self.request(query).await?;
        let result_set = self.get_result_set(response).await?;
        Ok(result_set.statistics)
    }

    /// Deletes the entire graph from the database.
    ///
    /// *This action is not easily reversible.*
    pub async fn delete(self) -> RedisGraphResult<()> {
        let graph_info = self.graph_info.lock().await;
        redis::cmd("GRAPH.DELETE")
            .arg(&graph_info.name)
            .query_async::<_, ()>(&mut self.conn.clone())
            .await
            .map_err(RedisGraphError::from)
    }

    /// Updates the internal label names by retrieving them from the database.
    ///
    /// There is no real need to call this function manually. This implementation
    /// updates the label names automatically when they become outdated.
    pub async fn update_labels(&self) -> RedisGraphResult<()> {
        let refresh_response = self.request("CALL db.labels()").await?;
        let labels = self.get_mapping(refresh_response).await?;
        (self.graph_info.lock().await).labels = labels;
        Ok(())
    }

    /// Updates the internal relationship type names by retrieving them from the database.
    ///
    /// There is no real need to call this function manually. This implementation
    /// updates the relationship type names automatically when they become outdated.
    pub async fn update_relationship_types(&self) -> RedisGraphResult<()> {
        let refresh_response = self.request("CALL db.relationshipTypes()").await?;
        let relationship_types = self.get_mapping(refresh_response).await?;
        (self.graph_info.lock().await).relationship_types = relationship_types;
        Ok(())
    }

    /// Updates the internal property key names by retrieving them from the database.
    ///
    /// There is no real need to call this function manually. This implementation
    /// updates the property key names automatically when they become outdated.
    pub async fn update_property_keys(&self) -> RedisGraphResult<()> {
        let refresh_response = self.request("CALL db.propertyKeys()").await?;
        let property_keys = self.get_mapping(refresh_response).await?;
        (self.graph_info.lock().await).property_keys = property_keys;
        Ok(())
    }

    async fn request(&self, query: &str) -> RedisGraphResult<Value> {
        let graph_info = self.graph_info.lock().await;
        redis::cmd("GRAPH.QUERY")
            .arg(&graph_info.name)
            .arg(query)
            .arg("--compact")
            .query_async(&mut self.conn.clone())
            .await
            .map_err(RedisGraphError::from)
    }

    fn get_result_set<'a>(
        &'a self,
        response: Value,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = RedisGraphResult<ResultSet>> + 'a>>
    {
        Box::pin(async move {
            let maybe_result_set = {
                let graph_info = self.graph_info.lock().await;
                ResultSet::from_redis_value_with_graph(response.clone(), &graph_info)
            };

            match maybe_result_set {
                Ok(result_set) => Ok(result_set),
                Err(RedisGraphError::LabelNotFound) => {
                    self.update_labels().await?;
                    self.get_result_set(response).await
                }
                Err(RedisGraphError::RelationshipTypeNotFound) => {
                    self.update_relationship_types().await?;
                    self.get_result_set(response).await
                }
                Err(RedisGraphError::PropertyKeyNotFound) => {
                    self.update_property_keys().await?;
                    self.get_result_set(response).await
                }
                any_err => any_err,
            }
        })
    }

    async fn get_mapping(&self, response: Value) -> RedisGraphResult<Vec<RedisString>> {
        let graph_info = self.graph_info.lock().await;
        let mut result_set = ResultSet::from_redis_value_with_graph(response, &graph_info)?;
        match &mut result_set.columns[0] {
            Column::Scalars(scalars) => scalars
                .iter_mut()
                .map(|scalar| match scalar.take() {
                    Scalar::String(string) => Ok(string),
                    _ => server_type_error!("expected strings in first column of result set"),
                })
                .collect::<RedisGraphResult<Vec<RedisString>>>(),
            _ => server_type_error!("expected scalars as first column in result set"),
        }
    }
}
