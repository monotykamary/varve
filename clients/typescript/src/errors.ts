export class VarveError extends Error {
  override readonly name: string = "VarveError";
  constructor(message: string, options?: ErrorOptions) { super(message, options); }
}
export class VarveValidationError extends VarveError { override readonly name = "VarveValidationError"; }
export class VarveConnectionError extends VarveError { override readonly name = "VarveConnectionError"; }
export class VarveAuthenticationError extends VarveError { override readonly name = "VarveAuthenticationError"; }
export class VarveProtocolError extends VarveError { override readonly name = "VarveProtocolError"; }
export class VarveClientLimitError extends VarveError { override readonly name = "VarveClientLimitError"; readonly admitted = false; }
export class VarveRequestTimeoutError extends VarveError { override readonly name = "VarveRequestTimeoutError"; readonly admitted = true; }
export class VarveAbortError extends VarveError {
  override readonly name = "AbortError";
  readonly admitted: boolean;
  constructor(message: string, admitted: boolean, options?: ErrorOptions) { super(message, options); this.admitted = admitted; }
}
export class VarveRpcError extends VarveError {
  override readonly name: string = "VarveRpcError";
  readonly code: number;
  readonly rpcId: string;
  readonly method: string;
  constructor(code: number, message: string, rpcId: string, method: string) {
    super(message); this.code = code; this.rpcId = rpcId; this.method = method;
  }
}
export class VarveAdmissionError extends VarveRpcError { override readonly name = "VarveAdmissionError"; readonly admitted = false; }
export interface AmbiguousOutcomeContext { method: string; rpcId: string; requestId?: string; }
export class VarveAmbiguousOutcomeError extends VarveError {
  override readonly name = "VarveAmbiguousOutcomeError";
  readonly ambiguous = true;
  readonly admitted = true;
  readonly method: string;
  readonly rpcId: string;
  readonly requestId: string | undefined;
  constructor(message: string, context: AmbiguousOutcomeContext, options?: ErrorOptions) {
    super(message, options); this.method = context.method; this.rpcId = context.rpcId; this.requestId = context.requestId;
  }
}
