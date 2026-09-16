use std::time::Duration;

#[derive(Clone, Debug, thiserror::Error, PartialEq, Eq)]
#[error("server error {code}: {message}")]
pub struct ServerError {
    pub code: i64,
    pub message: String,
}

impl ServerError {
    pub const OPERATION_FAILURE: i64 = -32000;
    pub const UNAUTHORIZED: i64 = -32001;
    pub const ADMISSION_REJECTED: i64 = -32003;
}

#[derive(Clone, Debug, thiserror::Error, PartialEq, Eq)]
pub enum Error {
    #[error("client is closed")]
    Closed,
    #[error("client is disconnected: {0}")]
    Disconnected(String),
    #[error("too many calls are already pending")]
    TooManyPending,
    #[error("pending calls exceed the configured aggregate byte limit")]
    PendingBytesExceeded,
    #[error("request exceeded the {0:?} deadline")]
    RequestTimeout(Duration),
    #[error("message is {actual} bytes; configured maximum is {maximum} bytes")]
    MessageTooLarge { actual: usize, maximum: usize },
    #[error("invalid request: {0}")]
    InvalidRequest(String),
    #[error("protocol error: {0}")]
    Protocol(String),
    #[error("transport error: {0}")]
    Transport(String),
    #[error(transparent)]
    Server(#[from] ServerError),
}

#[derive(Clone, Debug, thiserror::Error, PartialEq, Eq)]
pub enum ConnectError {
    #[error("invalid client configuration: {0}")]
    InvalidConfig(&'static str),
    #[error("invalid WebSocket URL: {0}")]
    InvalidUrl(&'static str),
    #[error("WebSocket connection exceeded the {0:?} deadline")]
    Timeout(Duration),
    #[error("WebSocket handshake failed: {0}")]
    Handshake(String),
    #[error("server did not negotiate the varve.v1 WebSocket subprotocol")]
    Subprotocol,
    #[error("authentication was rejected: {0}")]
    Authentication(ServerError),
    #[error("authentication protocol failed: {0}")]
    AuthenticationProtocol(String),
}

#[derive(Clone, Debug, thiserror::Error, PartialEq, Eq)]
pub enum AmbiguousWrite {
    #[error("request deadline elapsed after the call was accepted locally")]
    Timeout,
    #[error("connection ended after the call was accepted locally: {0}")]
    Disconnected(String),
    #[error("server reported an operation failure: {0}")]
    ServerOperation(ServerError),
    #[error("the response was not usable after the call was accepted locally: {0}")]
    Protocol(String),
}

#[derive(Clone, Debug, thiserror::Error, PartialEq, Eq)]
pub enum WriteError {
    #[error("write was not sent: {0}")]
    NotSent(Error),
    #[error("server rejected queue admission; the write did not commit: {0}")]
    AdmissionRejected(ServerError),
    #[error("server rejected the write: {0}")]
    Rejected(ServerError),
    #[error("write outcome is unknown for request ID {request_id}: {cause}")]
    OutcomeUnknown {
        request_id: crate::RequestId,
        cause: AmbiguousWrite,
    },
}

impl WriteError {
    pub fn may_have_committed(&self) -> bool {
        matches!(self, Self::OutcomeUnknown { .. })
    }
}
