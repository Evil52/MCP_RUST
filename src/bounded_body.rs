//! Size-bounded reading of outbound HTTP response bodies.
//!
//! Every outbound client previously carried its own copy of this loop. The
//! loop is the part worth sharing: it rejects an oversized `Content-Length`
//! before reading anything and stops as soon as the received bytes cross the
//! limit, so a compressed, chunked or mislabelled body cannot expand past it.
//! Limits and error types stay with each client.

use reqwest::Response;

/// Why a bounded read stopped.
#[derive(Debug)]
pub enum BoundedBodyError {
    /// `Content-Length` declared this many bytes, over the limit. Nothing was read.
    DeclaredTooLarge(u64),
    /// The stream crossed the limit. The count includes the chunk that crossed it.
    ReceivedTooLarge(u64),
    /// The body stream failed before completing.
    Transport(reqwest::Error),
}

impl BoundedBodyError {
    /// The declared or received size that exceeded the limit.
    pub const fn exceeded_bytes(&self) -> Option<u64> {
        match self {
            Self::DeclaredTooLarge(bytes) | Self::ReceivedTooLarge(bytes) => Some(*bytes),
            Self::Transport(_) => None,
        }
    }
}

/// Reads the whole body, failing once it would exceed `limit` bytes.
pub async fn read_bounded(
    response: &mut Response,
    limit: usize,
) -> Result<Vec<u8>, BoundedBodyError> {
    let declared = response.content_length();
    if let Some(declared) = declared
        && declared > limit as u64
    {
        return Err(BoundedBodyError::DeclaredTooLarge(declared));
    }
    let capacity = declared
        .and_then(|length| usize::try_from(length).ok())
        .unwrap_or(0)
        .min(limit);
    let mut body = Vec::with_capacity(capacity);
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(BoundedBodyError::Transport)?
    {
        let received = body.len().saturating_add(chunk.len());
        if received > limit {
            return Err(BoundedBodyError::ReceivedTooLarge(
                u64::try_from(received).unwrap_or(u64::MAX),
            ));
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        convert::Infallible,
        pin::Pin,
        task::{Context, Poll},
    };

    use axum::body::Bytes;
    use http_body::Frame;
    use reqwest::Body;

    use super::*;

    /// A body without a declared length, delivered in the given chunks.
    struct Chunked(VecDeque<Bytes>);

    impl http_body::Body for Chunked {
        type Data = Bytes;
        type Error = Infallible;

        fn poll_frame(
            mut self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
            Poll::Ready(self.0.pop_front().map(|chunk| Ok(Frame::data(chunk))))
        }
    }

    fn declared(body: &[u8]) -> Response {
        Response::from(axum::http::Response::new(body.to_vec()))
    }

    fn chunked(chunks: &[&[u8]]) -> Response {
        let chunks = chunks
            .iter()
            .map(|chunk| Bytes::copy_from_slice(chunk))
            .collect();
        Response::from(axum::http::Response::new(Body::wrap(Chunked(chunks))))
    }

    #[tokio::test]
    async fn a_body_at_the_limit_is_returned_whole() {
        let mut response = declared(b"abcd");
        assert_eq!(read_bounded(&mut response, 4).await.unwrap(), b"abcd");

        let mut response = chunked(&[b"ab", b"cd"]);
        assert_eq!(response.content_length(), None);
        assert_eq!(read_bounded(&mut response, 4).await.unwrap(), b"abcd");
    }

    #[tokio::test]
    async fn an_oversized_declared_length_is_rejected_before_reading() {
        let mut response = declared(b"abcde");
        let error = read_bounded(&mut response, 4).await.unwrap_err();
        assert!(
            matches!(error, BoundedBodyError::DeclaredTooLarge(5)),
            "{error:?}"
        );
        assert_eq!(error.exceeded_bytes(), Some(5));
        assert_eq!(
            response.chunk().await.unwrap().as_deref(),
            Some(&b"abcde"[..])
        );
    }

    #[tokio::test]
    async fn an_undeclared_stream_stops_at_the_chunk_that_crosses_the_limit() {
        let mut response = chunked(&[b"abc", b"de", b"fgh"]);
        let error = read_bounded(&mut response, 4).await.unwrap_err();
        assert!(
            matches!(error, BoundedBodyError::ReceivedTooLarge(5)),
            "{error:?}"
        );
        assert_eq!(error.exceeded_bytes(), Some(5));
        assert_eq!(
            response.chunk().await.unwrap().as_deref(),
            Some(&b"fgh"[..])
        );
    }
}
