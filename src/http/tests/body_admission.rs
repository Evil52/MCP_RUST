use super::{
    Arc, Body, Bytes, Context, Frame, HttpBody, Infallible, MCP_REQUEST_BODY_LIMIT_BYTES,
    McpBodyReadFailure, Pin, Poll, SizeHint, read_bounded_mcp_body,
};

struct HintedBody {
    lower_bound: u64,
    polls: Arc<std::sync::atomic::AtomicUsize>,
}

impl HttpBody for HintedBody {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(
        self: Pin<&mut Self>,
        _context: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        self.polls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Poll::Ready(None)
    }

    fn size_hint(&self) -> SizeHint {
        let mut hint = SizeHint::new();
        hint.set_lower(self.lower_bound);
        hint
    }
}

struct OversizedFrameBody;

impl HttpBody for OversizedFrameBody {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(
        self: Pin<&mut Self>,
        _context: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        Poll::Ready(Some(Ok(Frame::data(Bytes::from(vec![
            0;
            MCP_REQUEST_BODY_LIMIT_BYTES
                + 1
        ])))))
    }
}

struct TrailersBody {
    emitted: bool,
}

impl HttpBody for TrailersBody {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        _context: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        if self.emitted {
            return Poll::Ready(None);
        }
        self.emitted = true;
        Poll::Ready(Some(Ok(Frame::trailers(axum::http::HeaderMap::new()))))
    }
}

#[tokio::test]
async fn bounded_body_reader_ignores_non_data_frames() {
    let body = Body::new(TrailersBody { emitted: false });
    let bytes = read_bounded_mcp_body(body)
        .await
        .ok()
        .expect("trailers are not a transport failure");
    assert!(bytes.is_empty());
}

#[tokio::test]
async fn bounded_body_reader_polls_only_hints_within_the_request_budget() {
    for lower_bound in [0, (MCP_REQUEST_BODY_LIMIT_BYTES + 1) as u64] {
        let polls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let body = Body::new(HintedBody {
            lower_bound,
            polls: Arc::clone(&polls),
        });
        let result = read_bounded_mcp_body(body).await;
        if lower_bound == 0 {
            assert!(
                matches!(result, Ok(bytes) if bytes.is_empty()),
                "an admissible empty stream must be read"
            );
            assert_eq!(polls.load(std::sync::atomic::Ordering::SeqCst), 1);
        } else {
            assert!(matches!(result, Err(McpBodyReadFailure::TooLarge)));
            assert_eq!(polls.load(std::sync::atomic::Ordering::SeqCst), 0);
        }
    }
}

#[tokio::test]
async fn bounded_body_reader_rejects_a_frame_that_exhausts_the_remaining_budget() {
    let error = read_bounded_mcp_body(Body::new(OversizedFrameBody))
        .await
        .expect_err("an oversized frame cannot fit the request budget");
    assert!(matches!(error, McpBodyReadFailure::TooLarge));
}
