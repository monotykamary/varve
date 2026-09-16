export { connect } from "./client.js";
export {
  VarveAbortError,
  VarveAdmissionError,
  VarveAmbiguousOutcomeError,
  VarveAuthenticationError,
  VarveClientLimitError,
  VarveConnectionError,
  VarveError,
  VarveProtocolError,
  VarveRequestTimeoutError,
  VarveRpcError,
  VarveValidationError,
} from "./errors.js";
export type { AmbiguousOutcomeContext } from "./errors.js";
export type {
  CloseOptions, ConnectOptions, Int64, JsonInteger, JsonObject, JsonPrimitive, JsonValue,
  RequestOptions, Row, Status, TableConfig, VarveClient, WebSocketConstructor, WebSocketLike, WriteReceipt,
} from "./types.js";
