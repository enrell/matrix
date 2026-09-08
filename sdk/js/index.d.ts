/* Type declarations for the Matrix Node.js SDK (ML1, experimental 0.x).
 * Plain JS users never need a compiler; TS users get checked types. */

export const PROTOCOL_ID: "matrix.component";
export const PROTOCOL_VERSION: "0.1";
export const DEFAULT_MAX_FRAME: number;

export class SdkError extends Error {
  code: string;
  phase: string;
  detail: string;
  constructor(code: string, phase: string, message: string);
}
export class DepError extends SdkError {
  constructor(code: string, message: string);
}
export class ResError extends SdkError {
  constructor(code: string, message: string);
}
export class BootstrapError extends SdkError {}
export class OperatorError extends SdkError {
  constructor(code: string, message: string);
}

export function genEq(a: string | number | bigint, b: string | number | bigint): boolean;
export function genParse(v: string | number | bigint): bigint;

export interface DepBinding {
  id: string;
  capability: string;
}

export class CallCtx {
  readonly ticket: string;
  readonly sessionId: string;
  eventDroppedCount(): number;
  pendingStreamCount(): number;
  dependencies(): DepBinding[];
  sendStream(streamId: string, seq: number | bigint, payload: string): Promise<void>;
  invokeDependency(binding: string, input: unknown, timeoutS: number): Promise<unknown>;
  acquireResource(kind: string, label: string, intervalMs?: number): Promise<bigint>;
  releaseResource(handle: bigint | string | number): Promise<void>;
}

export class Handler {
  onCall(ctx: CallCtx, ticket: string, cap: string, input: unknown, signal: AbortSignal): Promise<unknown>;
  onCancel(ticket: string): void;
  onEvent(topic: string, payload: unknown): void;
  onStream(streamId: string, seq: bigint, payload: string): void;
}

export class Component {
  static connect(sockPath: string, logical: string): Promise<Component>;
  serve(handler: Handler): Promise<"dispose" | "eof" | string>;
}

export interface OperatorPki {
  ca: string;
  cert: string;
  key: string;
}

export class Client {
  request(action: Record<string, unknown>, timeoutS?: number): unknown;
  activate(component: string, ttlMs?: number, timeoutS?: number): { lease: string; fence: string;[k: string]: unknown };
  status(lease: string, fence: string | number, timeoutS?: number): unknown;
  invoke(lease: string, fence: string | number, operation: string, cap: string, input: unknown, timeoutS?: number): unknown;
  release(lease: string, fence: string | number, timeoutS?: number): unknown;
  renew(lease: string, fence: string | number, ttlMs?: number, timeoutS?: number): unknown;
  waitReady(lease: string, fence: string | number, timeoutS?: number): Promise<unknown>;
  close(): void;
}

export class OwnedKernel {
  readonly epoch: unknown;
  readonly api: string;
  readonly profile: string;
  readonly client: Client;
  readonly listen: string;
  close(): Promise<void>;
}

export function connect(binary: string, listen: string, ca: string, cert: string, key: string, serverName?: string): Client;
export function start(binary: string, config: Record<string, unknown>, operatorPki: OperatorPki, serverName?: string): Promise<OwnedKernel>;

export interface DoctorReport {
  node: string;
  binary: string;
  binaryFound: boolean;
  binaryExecutable: boolean;
  cliShapeOk: boolean;
  openssl: boolean;
  bwrap: boolean;
  socketDirWritable: boolean;
  errors: string[];
}
export function doctor(binary?: string): Promise<DoctorReport>;
