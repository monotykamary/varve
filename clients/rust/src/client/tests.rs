use super::*;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio_tungstenite::tungstenite::Error as SocketError;

fn fixture(
    max_pending_calls: usize,
    max_pending_bytes: usize,
) -> (Client, mpsc::Receiver<Command>) {
    let config = ClientConfig {
        max_pending_calls,
        max_pending_bytes,
        ..ClientConfig::default()
    };
    let (commands, receiver) = mpsc::channel(config.max_pending_calls);
    let client = Client {
        inner: Arc::new(Inner {
            commands,
            pending: Arc::new(Mutex::new(HashMap::new())),
            permits: Arc::new(Semaphore::new(config.max_pending_calls)),
            byte_permits: Arc::new(Semaphore::new(config.max_pending_bytes)),
            next_id: AtomicU64::new(1),
            closed: Arc::new(AtomicBool::new(false)),
            task: AsyncMutex::new(None),
            config,
        }),
    };
    (client, receiver)
}

fn ping_bytes() -> usize {
    serde_json::to_string(&RpcRequest {
        jsonrpc: "2.0",
        id: "rust-1",
        method: "ping",
        params: json!({}),
    })
    .unwrap()
    .len()
}

#[derive(Default)]
struct WriterState {
    flush: bool,
    fail: bool,
    buffered_bytes: usize,
    dropped_with_bytes: bool,
}

struct BlockedSocket {
    state: Arc<Mutex<WriterState>>,
    bytes: Arc<Semaphore>,
    budget: usize,
    buffered: Option<Message>,
}

impl Sink<Message> for BlockedSocket {
    type Error = SocketError;

    fn poll_ready(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(mut self: Pin<&mut Self>, message: Message) -> Result<(), Self::Error> {
        assert!(self.buffered.is_none());
        self.state.lock().unwrap().buffered_bytes = message.len();
        self.buffered = Some(message);
        Ok(())
    }

    fn poll_flush(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let state = self.state.lock().unwrap();
        // Check the real call's credit while the sink itself owns the payload.
        assert!(self.bytes.available_permits() <= self.budget - state.buffered_bytes);
        if state.fail {
            return Poll::Ready(Err(SocketError::ConnectionClosed));
        }
        if !state.flush {
            return Poll::Pending;
        }
        drop(state);
        self.buffered = None;
        self.state.lock().unwrap().buffered_bytes = 0;
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.poll_flush(cx)
    }
}

impl Stream for BlockedSocket {
    type Item = Result<Message, SocketError>;

    fn poll_next(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Pending
    }
}

impl Drop for BlockedSocket {
    fn drop(&mut self) {
        if let Some(message) = self.buffered.take() {
            assert!(self.bytes.available_permits() <= self.budget - message.len());
            self.state.lock().unwrap().dropped_with_bytes = true;
            drop(message);
        }
    }
}

fn writer(
    client: &Client,
    commands: mpsc::Receiver<Command>,
) -> (impl Future<Output = ()> + use<>, Arc<Mutex<WriterState>>) {
    let state = Arc::new(Mutex::new(WriterState::default()));
    let socket = BlockedSocket {
        state: Arc::clone(&state),
        bytes: Arc::clone(&client.inner.byte_permits),
        budget: client.inner.config.max_pending_bytes,
        buffered: None,
    };
    (
        connection_task(
            socket,
            commands,
            Arc::clone(&client.inner.pending),
            Arc::clone(&client.inner.closed),
            client.inner.config.request_timeout,
            client.inner.config.close_timeout,
        ),
        state,
    )
}

#[tokio::test]
async fn cancelled_calls_keep_queued_frames_charged_until_discarded() {
    let bytes = ping_bytes();
    let (client, mut commands) = fixture(4, bytes * 2);
    for _ in 0..2 {
        let mut call = Box::pin(client.ping());
        assert!(futures_util::poll!(&mut call).is_pending());
        drop(call);
    }
    assert!(lock_pending(&client.inner.pending).is_empty());
    assert_eq!(client.inner.permits.available_permits(), 4);
    assert_eq!(commands.len(), 2);
    // Correlation cancellation must not uncharge text still owned by the queue.
    assert_eq!(client.inner.byte_permits.available_permits(), 0);
    assert!(matches!(
        client.ping().await,
        Err(Error::PendingBytesExceeded)
    ));
    assert_eq!(commands.len(), 2);

    let retained = commands.try_recv().unwrap();
    let Command::Send { text, .. } = &retained else {
        panic!("expected a queued frame");
    };
    assert_eq!(text.len(), bytes);
    assert_eq!(client.inner.byte_permits.available_permits(), 0);
    drop(retained);
    assert_eq!(client.inner.byte_permits.available_permits(), bytes);

    let mut admitted = Box::pin(client.ping());
    assert!(futures_util::poll!(&mut admitted).is_pending());
    drop(admitted);
    assert_eq!(client.inner.byte_permits.available_permits(), 0);
    // Receiver shutdown discards every remaining frame, even after cancellation.
    drop(commands);
    assert_eq!(client.inner.byte_permits.available_permits(), bytes * 2);
    assert!(matches!(client.ping().await, Err(Error::Closed)));
    assert!(lock_pending(&client.inner.pending).is_empty());
    assert_eq!(client.inner.permits.available_permits(), 4);
    assert_eq!(client.inner.byte_permits.available_permits(), bytes * 2);
}

#[tokio::test]
async fn cancelled_writing_and_queued_frames_stay_charged_through_flush_and_close() {
    let bytes = ping_bytes();
    let (client, commands) = fixture(4, bytes * 2);
    let (task, state) = writer(&client, commands);
    let mut task = Box::pin(task);
    let mut writing = Box::pin(client.ping());
    let mut queued = Box::pin(client.ping());
    assert!(futures_util::poll!(&mut writing).is_pending());
    assert!(futures_util::poll!(&mut queued).is_pending());
    assert!(futures_util::poll!(&mut task).is_pending());
    assert_eq!(state.lock().unwrap().buffered_bytes, bytes);
    drop(writing);
    drop(queued);
    assert!(lock_pending(&client.inner.pending).is_empty());
    assert_eq!(client.inner.byte_permits.available_permits(), 0);
    assert!(matches!(
        client.ping().await,
        Err(Error::PendingBytesExceeded)
    ));

    state.lock().unwrap().flush = true;
    assert!(futures_util::poll!(&mut task).is_pending());
    assert_eq!(state.lock().unwrap().buffered_bytes, 0);
    assert_eq!(client.inner.byte_permits.available_permits(), bytes * 2);
    let mut admitted = Box::pin(client.ping());
    assert!(futures_util::poll!(&mut admitted).is_pending());
    let mut close = Box::pin(client.close());
    assert!(futures_util::poll!(&mut close).is_pending());
    assert!(futures_util::poll!(&mut task).is_ready());
    close.await.unwrap();
    assert!(matches!(admitted.await, Err(Error::Disconnected(_))));
    assert!(lock_pending(&client.inner.pending).is_empty());
    assert_eq!(client.inner.byte_permits.available_permits(), bytes * 2);
    assert_eq!(client.inner.permits.available_permits(), 4);
    assert!(matches!(client.ping().await, Err(Error::Closed)));
}

#[tokio::test]
async fn response_cleanup_does_not_release_unflushed_writer_credit() {
    let bytes = ping_bytes();
    let (client, commands) = fixture(2, bytes);
    let (task, state) = writer(&client, commands);
    let mut task = Box::pin(task);
    let mut call = Box::pin(client.ping());
    assert!(futures_util::poll!(&mut call).is_pending());
    assert!(futures_util::poll!(&mut task).is_pending());
    dispatch_response(
        r#"{"jsonrpc":"2.0","id":"rust-1","result":{"pong":true}}"#,
        &client.inner.pending,
    )
    .unwrap();
    call.await.unwrap();
    assert!(lock_pending(&client.inner.pending).is_empty());
    assert_eq!(client.inner.byte_permits.available_permits(), 0);
    assert!(matches!(
        client.ping().await,
        Err(Error::PendingBytesExceeded)
    ));
    state.lock().unwrap().flush = true;
    assert!(futures_util::poll!(&mut task).is_pending());
    assert_eq!(client.inner.byte_permits.available_permits(), bytes);
    drop(task);
}

#[tokio::test]
async fn flushed_frame_keeps_correlation_credit_until_response() {
    let bytes = ping_bytes();
    let (client, commands) = fixture(2, bytes);
    let (task, state) = writer(&client, commands);
    let mut task = Box::pin(task);
    state.lock().unwrap().flush = true;
    let mut call = Box::pin(client.ping());
    assert!(futures_util::poll!(&mut call).is_pending());
    assert!(futures_util::poll!(&mut task).is_pending());
    assert_eq!(state.lock().unwrap().buffered_bytes, 0);
    assert_eq!(client.inner.byte_permits.available_permits(), 0);
    assert!(matches!(
        client.ping().await,
        Err(Error::PendingBytesExceeded)
    ));
    dispatch_response(
        r#"{"jsonrpc":"2.0","id":"rust-1","result":{"pong":true}}"#,
        &client.inner.pending,
    )
    .unwrap();
    call.await.unwrap();
    assert_eq!(client.inner.byte_permits.available_permits(), bytes);
    drop(task);
}

#[tokio::test]
async fn failed_close_enqueue_still_joins_writer_and_releases_credit() {
    let bytes = ping_bytes();
    let (client, mut commands) = fixture(2, bytes);
    let mut call = Box::pin(client.ping());
    assert!(futures_util::poll!(&mut call).is_pending());
    commands.close();
    let (task, state) = writer(&client, commands);
    state.lock().unwrap().flush = true;
    *client.inner.task.lock().await = Some(tokio::spawn(task));
    assert!(matches!(client.close().await, Err(Error::Closed)));
    assert!(client.inner.task.lock().await.is_none());
    assert!(lock_pending(&client.inner.pending).is_empty());
    assert_eq!(client.inner.byte_permits.available_permits(), bytes);
    assert_eq!(client.inner.permits.available_permits(), 2);
    assert!(matches!(call.await, Err(Error::Disconnected(_))));
}

#[tokio::test]
async fn writer_failure_discards_buffer_and_queue_before_releasing_all_credit() {
    let bytes = ping_bytes();
    let (client, commands) = fixture(2, bytes * 2);
    let (task, state) = writer(&client, commands);
    let mut task = Box::pin(task);
    let mut first = Box::pin(client.ping());
    let mut second = Box::pin(client.ping());
    assert!(futures_util::poll!(&mut first).is_pending());
    assert!(futures_util::poll!(&mut second).is_pending());
    assert!(futures_util::poll!(&mut task).is_pending());
    drop(first);
    state.lock().unwrap().fail = true;
    assert!(futures_util::poll!(&mut task).is_ready());
    assert!(state.lock().unwrap().dropped_with_bytes);
    assert_eq!(client.inner.byte_permits.available_permits(), bytes * 2);
    assert_eq!(client.inner.permits.available_permits(), 2);
    assert!(lock_pending(&client.inner.pending).is_empty());
    assert!(matches!(second.await, Err(Error::Disconnected(_))));
    assert!(matches!(client.ping().await, Err(Error::Closed)));
}

#[tokio::test]
async fn writer_cancellation_discards_buffer_queue_and_pending_correlations() {
    let bytes = ping_bytes();
    let (client, commands) = fixture(2, bytes * 2);
    let (task, state) = writer(&client, commands);
    let mut task = Box::pin(task);
    let mut first = Box::pin(client.ping());
    let mut second = Box::pin(client.ping());
    assert!(futures_util::poll!(&mut first).is_pending());
    assert!(futures_util::poll!(&mut second).is_pending());
    assert!(futures_util::poll!(&mut task).is_pending());
    drop(first);
    // Dropping the task future exercises the same destructor path as task abort.
    drop(task);
    assert!(state.lock().unwrap().dropped_with_bytes);
    assert!(client.inner.closed.load(Ordering::Acquire));
    assert!(lock_pending(&client.inner.pending).is_empty());
    assert_eq!(client.inner.byte_permits.available_permits(), bytes * 2);
    assert_eq!(client.inner.permits.available_permits(), 2);
    assert!(matches!(second.await, Err(Error::Closed)));
}

#[tokio::test]
async fn unflushed_sink_is_dropped_before_its_last_credit_on_failure_or_abort() {
    for abort in [false, true] {
        let bytes = ping_bytes();
        let (client, commands) = fixture(2, bytes);
        let (task, state) = writer(&client, commands);
        let mut task = Box::pin(task);
        let mut call = Box::pin(client.ping());
        assert!(futures_util::poll!(&mut call).is_pending());
        assert!(futures_util::poll!(&mut task).is_pending());
        drop(call);
        assert_eq!(client.inner.byte_permits.available_permits(), 0);
        // No pending entry or other queued frame can mask early writer release
        // in BlockedSocket::drop's credit assertion.
        if !abort {
            state.lock().unwrap().fail = true;
            assert!(futures_util::poll!(&mut task).is_ready());
        }
        drop(task);
        assert!(state.lock().unwrap().dropped_with_bytes);
        assert_eq!(client.inner.byte_permits.available_permits(), bytes);
    }
}

#[tokio::test]
async fn full_queue_rejection_releases_only_the_rejected_frame_credit() {
    let bytes = ping_bytes();
    let (client, commands) = fixture(1, bytes * 2);
    let mut call = Box::pin(client.ping());
    assert!(futures_util::poll!(&mut call).is_pending());
    drop(call);
    assert!(matches!(client.ping().await, Err(Error::TooManyPending)));
    assert!(lock_pending(&client.inner.pending).is_empty());
    assert_eq!(client.inner.byte_permits.available_permits(), bytes);
    assert_eq!(client.inner.permits.available_permits(), 1);
    drop(commands);
    assert_eq!(client.inner.byte_permits.available_permits(), bytes * 2);
}

#[tokio::test]
async fn dropping_all_clients_drains_frames_and_releases_credit() {
    let bytes = ping_bytes();
    let (client, commands) = fixture(2, bytes);
    let credit = Arc::clone(&client.inner.byte_permits);
    let (task, state) = writer(&client, commands);
    let mut task = Box::pin(task);
    let mut call = Box::pin(client.ping());
    assert!(futures_util::poll!(&mut call).is_pending());
    drop(call);
    drop(client);
    assert_eq!(credit.available_permits(), 0);
    state.lock().unwrap().flush = true;
    assert!(futures_util::poll!(&mut task).is_ready());
    assert_eq!(credit.available_permits(), bytes);
}
