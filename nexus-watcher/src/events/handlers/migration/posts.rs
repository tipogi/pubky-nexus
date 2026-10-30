//! The first step: the user's posts. Every post they authored gets the `social/v1` address,
//! tombstones and posts the migrator skipped included, because Nexus can't tell which ones have
//! a copy and nothing fetches these addresses. `content` is not touched.
//!
//! The caches that hold the address are then rewritten, never deleted, because several readers
//! don't refill a missing key: `Post:Details` from the graph, JSON only, with no sorted-set
//! writes, and `Post:Relationships` of the replies and reposts of those posts, other users'
//! included, through its reindex.
//!
//! Paging is keyset on the post id, never `SKIP`, so a rerun repeats the Redis writes of the
//! posts whose graph write already landed.

use crate::events::EventProcessorError;
use nexus_common::db::graph::keyset_scan;
use nexus_common::db::{exec_single_row, fetch_all_rows_from_graph, queries, GraphError};
use nexus_common::models::post::{PostDetails, PostRelationships};
use pubky_social_specs::{post_uri_builder, PubkyId};
use tracing::debug;

/// Posts per batch: one graph write and one cache write each.
pub const BATCH_SIZE: usize = 100;

/// Migrates the posts `user_id` authored, in batches of `batch_size`, each batch its own
/// transaction. Running it again gives the same result.
pub async fn migrate(user_id: &PubkyId, batch_size: usize) -> Result<(), EventProcessorError> {
    debug!(%user_id, "Migrating posts");
    let author_id: &str = user_id.as_ref();
    keyset_scan(batch_size, "migration.posts", |after_id| async move {
        let page = queries::get::get_author_post_ids_page(author_id, &after_id, batch_size);
        let post_ids = fetch_all_rows_from_graph(page)
            .await?
            .iter()
            .map(|row| row.get::<String>("id").map_err(GraphError::from))
            .collect::<Result<Vec<_>, _>>()?;
        let Some(last_id) = post_ids.last().cloned() else {
            return Ok((0, None));
        };
        migrate_batch(author_id, &post_ids, batch_size).await?;
        Ok((post_ids.len(), Some(last_id)))
    })
    .await
}

/// One batch: the addresses in the graph, then the caches that hold them.
async fn migrate_batch(
    author_id: &str,
    post_ids: &[String],
    batch_size: usize,
) -> Result<(), EventProcessorError> {
    let posts: Vec<(String, String)> = post_ids
        .iter()
        .map(|post_id| {
            let uri = post_uri_builder(author_id.to_string(), post_id.clone());
            (post_id.clone(), uri)
        })
        .collect();
    exec_single_row(queries::put::set_post_uris(author_id, &posts)).await?;

    let details = PostDetails::get_many_from_graph(author_id, post_ids).await?;
    PostDetails::put_many_to_index(author_id, details).await?;

    reindex_children_relationships(author_id, post_ids, batch_size).await
}

/// Rebuilds `Post:Relationships` of every reply and repost of `post_ids`, whoever wrote them,
/// in pages of `batch_size` keyed on the child's id.
async fn reindex_children_relationships(
    author_id: &str,
    post_ids: &[String],
    batch_size: usize,
) -> Result<(), EventProcessorError> {
    keyset_scan(
        batch_size,
        "migration.posts.children",
        |after_id| async move {
            let page =
                queries::get::get_post_children_page(author_id, post_ids, &after_id, batch_size);
            let rows = fetch_all_rows_from_graph(page).await?;
            let mut last_id = None;
            for row in &rows {
                let child_author_id: String = row.get("author_id").map_err(GraphError::from)?;
                let child_post_id: String = row.get("post_id").map_err(GraphError::from)?;
                PostRelationships::reindex(&child_author_id, &child_post_id).await?;
                last_id = Some(child_post_id);
            }
            Ok((rows.len(), last_id))
        },
    )
    .await
}
