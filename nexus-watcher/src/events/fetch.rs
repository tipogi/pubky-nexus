use crate::EventProcessorError;
pub(crate) use pubky_watcher::read_stream_capped;
use pubky_watcher::ClientResponse;

/// Max bytes to read from an error response body.
pub(crate) const MAX_ERROR_BODY: usize = 4 * 1024;

/// Max bytes for a JSON resource descriptor (user, post, tag, file meta, etc).
pub(crate) const MAX_RESOURCE_SIZE: usize = 2 * 1024 * 1024;

/// Max bytes for a homeserver `/events` response body.
/// Worst case: 1 000 events × 4 160 bytes/line ≈ 4 MiB; 5 MiB gives headroom.
pub(crate) const MAX_EVENTS_BODY: usize = 5 * 1024 * 1024;

/// Truncates a byte slice to `max` bytes for safe embedding in error messages.
pub(crate) fn format_error_body(bytes: &[u8], max: usize) -> String {
    if bytes.len() > max {
        format!("{}… (truncated)", String::from_utf8_lossy(&bytes[..max]))
    } else {
        String::from_utf8_lossy(bytes).into_owned()
    }
}

/// Fetches a client response body, enforcing a size limit.
///
/// 1. If `Content-Length` is present and exceeds `max`, rejects immediately.
/// 2. Otherwise, streams through [read_stream_capped] to catch lying/missing
///    `Content-Length` headers.
///
/// Returns [EventProcessorError::FetchSizeExceeded] on size violation,
/// [`EventProcessorError::ClientError`] on stream failure.
pub(crate) async fn fetch_capped(
    resp: ClientResponse,
    max: u64,
) -> Result<Vec<u8>, EventProcessorError> {
    if let Some(cl) = resp.content_length {
        if cl > max {
            return Err(EventProcessorError::FetchSizeExceeded(cl, max));
        }
    }
    let (buf, exceeded) = read_stream_capped(resp.body, max as usize)
        .await
        .map_err(|e| EventProcessorError::client_error(e.to_string()))?;
    if exceeded {
        return Err(EventProcessorError::FetchSizeExceeded(
            buf.len() as u64,
            max,
        ));
    }
    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use futures::stream;
    use pubky::StatusCode;
    use pubky_watcher::{ClientError, ClientResponse};

    fn response(total: usize, content_length: Option<u64>) -> ClientResponse {
        ClientResponse {
            status: StatusCode::OK,
            content_length,
            content_type: None,
            body: Box::pin(stream::iter(vec![Ok::<Bytes, ClientError>(Bytes::from(
                vec![0xAB; total],
            ))])),
        }
    }

    fn no_cl_oversized(total: usize) -> ClientResponse {
        response(total, None)
    }

    fn high_cl_response(total: usize) -> ClientResponse {
        response(total, Some(total as u64))
    }

    #[tokio::test]
    async fn fetch_capped_precheck_rejects_high_cl() {
        let resp = high_cl_response(10_000);
        // Guard: the precheck relies on `Content-Length` being reflected here.
        assert_eq!(resp.content_length, Some(10_000));

        let err = fetch_capped(resp, 100).await.unwrap_err();
        assert!(matches!(
            err,
            EventProcessorError::FetchSizeExceeded(10_000, 100)
        ));
    }

    #[tokio::test]
    async fn fetch_capped_stream_rejects_absent_cl_oversized() {
        let r = no_cl_oversized(200);
        assert!(
            r.content_length.is_none(),
            "guard: else this re-tests the pre-check"
        );
        let err = fetch_capped(r, 100).await.unwrap_err();
        assert!(matches!(
            err,
            EventProcessorError::FetchSizeExceeded(_, 100)
        ));
    }

    #[tokio::test]
    async fn fetch_capped_accepts_under_cap_stream() {
        assert!(fetch_capped(no_cl_oversized(50), 100).await.is_ok());
    }
}
