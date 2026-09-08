"use strict";
/* Matrix component SDK for Node.js (ML1, zero dependencies).
 *
 * Speaks `matrix.component/0.1` with the local host: handshake,
 * registration, activation, and call serving with cooperative
 * cancellation. Mirrors the reference SDKs (Rust `matrix-component`,
 * Python `matrix_component`): same observable behavior.
 *
 * Concurrency notes (epic table: one shared runtime, no blocking the
 * reader): all waits yield to the event loop, so there is no reader
 * thread to guard — handlers must still never busy-block, and
 * `on_event`/`on_stream` must observe fast (bounded queue, drop-oldest
 * counted, backpressure stays host-managed).
 */

const net = require("node:net");

const PROTOCOL_ID = "matrix.component";
const PROTOCOL_VERSION = "0.1";
const DEFAULT_MAX_FRAME = 1024 * 1024;
const EVENT_CAP = 64;

let nextId = 1;
function fresh(prefix) {
  return `${prefix}-${nextId++}`;
}

class SdkError extends Error {
  constructor(code, phase, message) {
    super(`${code} [${phase}]: ${message}`);
    this.code = code;
    this.phase = phase;
    this.detail = message;
  }
}

class DepError extends SdkError {
  constructor(code, message) {
    super(code, "dependency", message);
  }
}

class ResError extends SdkError {
  constructor(code, message) {
    super(code, "resource", message);
  }
}

function genEq(a, b) {
  try {
    return BigInt(String(a)) === BigInt(String(b));
  } catch {
    return false;
  }
}

function genParse(v) {
  return BigInt(String(v));
}

/* Length-prefixed JSON framing over a Unix socket. Frames are only
 * consumed when a reader waits: coalesced arrivals stay buffered for
 * the next read instead of being dropped. */
class Framing {
  constructor(sock, maxFrame) {
    this.sock = sock;
    this.maxFrame = maxFrame;
    this.buf = Buffer.alloc(0);
    this.waiters = []; // {resolve, reject}
    this.ended = false;
    this.failed = null;
    sock.on("data", (d) => {
      this.buf = Buffer.concat([this.buf, d]);
      this._pump();
    });
    sock.on("end", () => this._end());
    sock.on("close", () => this._end());
    sock.on("error", (e) => this._fail(e));
  }
  _pump() {
    while (this.waiters.length > 0) {
      if (this.buf.length < 4) return;
      const n = this.buf.readUInt32BE(0);
      if (n === 0 || n > this.maxFrame) {
        this._fail(new Error(`bad frame length ${n}`));
        return;
      }
      if (this.buf.length < 4 + n) return;
      const payload = this.buf.subarray(4, 4 + n);
      this.buf = this.buf.subarray(4 + n);
      let msg;
      try {
        msg = JSON.parse(payload.toString("utf-8"));
      } catch {
        msg = { __malformed: true }; // drop silently, keep session
      }
      this.waiters.shift().resolve(msg);
    }
  }
  _end() {
    this.ended = true;
    const ws = this.waiters;
    this.waiters = [];
    for (const w of ws) w.resolve(null);
  }
  _fail(e) {
    if (this.failed) return;
    this.failed = e;
    const ws = this.waiters;
    this.waiters = [];
    for (const w of ws) w.reject(e);
  }
  read() {
    if (this.failed) return Promise.reject(this.failed);
    if (this.ended) return Promise.resolve(null);
    return new Promise((resolve, reject) => {
      this.waiters.push({ resolve, reject });
      this._pump();
    });
  }
  write(obj) {
    const raw = Buffer.from(JSON.stringify(obj), "utf-8");
    if (raw.length > this.maxFrame)
      return Promise.reject(new Error("frame above max"));
    const frame = Buffer.alloc(4 + raw.length);
    frame.writeUInt32BE(raw.length, 0);
    raw.copy(frame, 4);
    return new Promise((resolve, reject) => {
      this.sock.write(frame, (e) => (e ? reject(e) : resolve()));
    });
  }
}

class CallCtx {
  constructor(comp, ticket, cancel) {
    this._comp = comp;
    this.ticket = ticket;
    this._cancel = cancel; // AbortController
  }
  get sessionId() {
    return this._comp.sessionId;
  }
  eventDroppedCount() {
    return this._comp.evDropped;
  }
  pendingStreamCount() {
    let n = 0;
    for (const it of this._comp.evQueue) if (it.kind === "stream") n++;
    return n;
  }
  dependencies() {
    return this._comp.depBindings.map((b) => ({ ...b }));
  }
  async sendStream(streamId, seq, payload) {
    if (typeof payload !== "string")
      throw new SdkError("invalid-message", "stream",
        "stream payloads are text; binary must be refused, never lossy-converted");
    const s = typeof seq === "bigint" ? seq.toString() : String(seq);
    if (!streamId || !/^[0-9]+$/.test(s))
      throw new SdkError("invalid-message", "stream", "bad stream id/seq");
    await this._comp._send({
      protocol: PROTOCOL_ID, version: PROTOCOL_VERSION, type: "stream.data",
      message_id: `m-${streamId}-${s}`, session_id: this._comp.sessionId,
      instance_id: this._comp.instanceId, generation: this._comp.generationStr,
      body: { stream_id: streamId, seq: s, payload },
    });
  }
  async invokeDependency(binding, input, timeoutS) {
    if (!this._comp.features.includes("dependency-calls/1"))
      throw new DepError("unsupported-feature", "dependency calls not negotiated");
    const timeoutMs = Math.floor(timeoutS * 1000);
    if (!(timeoutMs > 0))
      throw new DepError("invalid-message", "timeout must be positive");
    const rid = fresh("r-dep");
    const waiter = {};
    const done = new Promise((resolve, reject) => {
      waiter.resolve = resolve;
      waiter.reject = reject;
    });
    this._comp.depWaiters.set(rid, waiter);
    try {
      await this._comp._send({
        protocol: PROTOCOL_ID, version: PROTOCOL_VERSION, type: "dependency.open",
        message_id: fresh("m-dep"), session_id: this._comp.sessionId,
        instance_id: this._comp.instanceId, generation: this._comp.generationStr,
        request_id: rid,
        body: { parent_ticket: this.ticket, binding_id: binding,
                timeout_ms: timeoutMs, input: input === undefined ? {} : input },
      });
    } catch (e) {
      this._comp.depWaiters.delete(rid);
      throw new DepError("internal", `send: ${e.message}`);
    }
    const deadline = Date.now() + timeoutS * 1000 + 10000;
    const onAbort = () => {
      this._comp.depWaiters.delete(rid);
      this._cancelDep(rid).catch(() => {});
      waiter.reject(new DepError("cancelled", "parent cancelled"));
    };
    if (this._cancel.signal.aborted) {
      onAbort();
    } else {
      this._cancel.signal.addEventListener("abort", onAbort, { once: true });
    }
    const timer = setTimeout(() => {
      this._comp.depWaiters.delete(rid);
      this._cancelDep(rid).catch(() => {});
      waiter.reject(new DepError("outcome-unknown", "sdk wait timeout"));
    }, Math.max(0, deadline - Date.now()));
    try {
      return await done;
    } finally {
      clearTimeout(timer);
      this._cancel.signal.removeEventListener("abort", onAbort);
      this._comp.depWaiters.delete(rid);
    }
  }
  async _cancelDep(target) {
    try {
      await this._comp._send({
        protocol: PROTOCOL_ID, version: PROTOCOL_VERSION, type: "dependency.cancel",
        message_id: fresh("m-dep-cancel"), session_id: this._comp.sessionId,
        instance_id: this._comp.instanceId, generation: this._comp.generationStr,
        request_id: fresh("r-dep-cancel"), body: { target_request_id: target },
      });
    } catch { /* fire-and-forget */ }
  }
  async _resourceRoundtrip(operation, fields) {
    const rid = fresh("r-res");
    const waiter = {};
    const done = new Promise((resolve, reject) => {
      waiter.resolve = resolve;
      waiter.reject = reject;
    });
    this._comp.resWaiters.set(rid, waiter);
    const body = { operation_id: fresh("op-res"), ...fields };
    try {
      await this._comp._send({
        protocol: PROTOCOL_ID, version: PROTOCOL_VERSION, type: `resource.${operation}`,
        message_id: fresh("m-res"), session_id: this._comp.sessionId,
        instance_id: this._comp.instanceId, generation: this._comp.generationStr,
        request_id: rid, body,
      });
    } catch (e) {
      this._comp.resWaiters.delete(rid);
      throw new ResError("internal", `send: ${e.message}`);
    }
    const onAbort = () => {
      this._comp.resWaiters.delete(rid);
      waiter.reject(new ResError("cancelled", "parent cancelled"));
    };
    if (this._cancel.signal.aborted) onAbort();
    else this._cancel.signal.addEventListener("abort", onAbort, { once: true });
    const timer = setTimeout(() => {
      this._comp.resWaiters.delete(rid);
      waiter.reject(new ResError("outcome-unknown", "resource wait timeout"));
    }, 10000);
    try {
      return await done;
    } finally {
      clearTimeout(timer);
      this._cancel.signal.removeEventListener("abort", onAbort);
      this._comp.resWaiters.delete(rid);
    }
  }
  async acquireResource(kind, label, intervalMs) {
    const fields = { kind: String(kind), label: String(label) };
    if (intervalMs !== undefined && intervalMs !== null)
      fields.interval_ms = Math.floor(intervalMs);
    const extra = await this._resourceRoundtrip("acquire", fields);
    const h = extra.handle;
    if (typeof h !== "string" || !/^[0-9]+$/.test(h))
      throw new ResError("internal", "missing handle");
    return BigInt(h);
  }
  async releaseResource(handle) {
    const s = typeof handle === "bigint" ? handle.toString() : String(handle);
    await this._resourceRoundtrip("release", { handle: s });
  }
}

/* Handler base: override onCall; optionally onCancel/onEvent/onStream. */
class Handler {
  async onCall(ctx, ticket, cap, input, signal) {
    throw new SdkError("internal", "handler", "onCall not implemented");
  }
  onCancel(ticket) {}
  onEvent(topic, payload) {}
  onStream(streamId, seq, payload) {}
}

class Component {
  constructor(sock, framing, sessionId, instanceId, generationStr, maxFrame, features, depBindings) {
    this.sock = sock;
    this.framing = framing;
    this.sessionId = sessionId;
    this.instanceId = instanceId;
    this.generationStr = generationStr;
    this.maxFrame = maxFrame;
    this.features = features;
    this.depBindings = depBindings;
    this.depWaiters = new Map();
    this.resWaiters = new Map();
    this.calls = new Map(); // ticket -> AbortController
    this.evQueue = [];
    this.evDropped = 0;
    this._writeChain = Promise.resolve();
    this._dispatchScheduled = false;
    this._handler = null;
  }
  _send(obj) {
    const t = this._writeChain.then(() => this.framing.write(obj));
    this._writeChain = t.catch(() => {});
    return t;
  }
  static async connect(sockPath, logical) {
    const token = process.env.MATRIX_LAUNCH_TOKEN || "";
    const sock = net.createConnection(sockPath);
    await new Promise((resolve, reject) => {
      sock.once("connect", resolve);
      sock.once("error", reject);
    });
    const framing = new Framing(sock, DEFAULT_MAX_FRAME);
    const send0 = (o) => framing.write(o);
    await send0({
      protocol: PROTOCOL_ID, version: PROTOCOL_VERSION, type: "hello",
      message_id: "h1",
      body: { launch_token: token, versions: ["0.1"], max_frame: DEFAULT_MAX_FRAME,
              client: "matrix-component-js", features: ["dependency-calls/1"] },
    });
    const welcome = await framing.read();
    if (!welcome || welcome.type !== "welcome") throw new Error(`expected welcome, got ${JSON.stringify(welcome)}`);
    const sessionId = welcome.session_id;
    const maxFrame = Number((welcome.body || {}).max_frame || DEFAULT_MAX_FRAME);
    framing.maxFrame = maxFrame;
    const features = (((welcome.body || {}).features) || []).filter((f) => typeof f === "string");
    await send0({
      protocol: PROTOCOL_ID, version: PROTOCOL_VERSION, type: "component.register",
      message_id: "reg1", session_id: sessionId, body: { manifest: { id: logical } },
    });
    const reg = await framing.read();
    if (!reg || reg.type !== "registered") throw new Error(`register rejected: ${JSON.stringify(reg)}`);
    const comp = new Component(sock, framing, sessionId, reg.instance_id,
      String(reg.generation), maxFrame, features, []);
    const act = await framing.read();
    if (!act || act.type !== "lifecycle.activate")
      throw new Error(`expected activate, got ${act && act.type}`);
    const rawBindings = ((act.body || {}).dependency_bindings) || [];
    comp.depBindings = rawBindings
      .filter((b) => b && b.binding_id && b.capability)
      .map((b) => ({ id: String(b.binding_id), capability: String(b.capability) }));
    const op = ((act.body || {}).operation_id) || "op?";
    await send0({
      protocol: PROTOCOL_ID, version: PROTOCOL_VERSION, type: "lifecycle.result",
      message_id: "lc1", session_id: sessionId, instance_id: comp.instanceId,
      generation: comp.generationStr, request_id: act.request_id,
      body: { operation_id: op, status: "ok", pending: [] },
    });
    return comp;
  }
  _boundOk(env) {
    return env.session_id === this.sessionId &&
      (env.instance_id === undefined || String(env.instance_id) === String(this.instanceId)) &&
      (env.generation === undefined || genEq(env.generation, this.generationStr));
  }
  _enqueue(item) {
    if (this.evQueue.length >= EVENT_CAP) {
      this.evQueue.shift();
      this.evDropped++;
    }
    this.evQueue.push(item);
    if (!this._dispatchScheduled) {
      this._dispatchScheduled = true;
      setImmediate(() => this._dispatch());
    }
  }
  async _dispatch() {
    this._dispatchScheduled = false;
    const batch = this.evQueue;
    this.evQueue = [];
    for (const it of batch) {
      try {
        if (it.kind === "stream") await this._handler.onStream(it.streamId, it.seq, it.payload);
        else await this._handler.onEvent(it.topic, it.payload);
      } catch { /* handler bugs never kill the session */ }
    }
    if (this.evQueue.length && !this._dispatchScheduled) {
      this._dispatchScheduled = true;
      setImmediate(() => this._dispatch());
    }
  }
  async _replyLifecycle(op, requestId) {
    await this._send({
      protocol: PROTOCOL_ID, version: PROTOCOL_VERSION, type: "lifecycle.result",
      message_id: `m-lc-${typeof op === "string" ? op : "op"}`,
      session_id: this.sessionId, instance_id: this.instanceId,
      generation: this.generationStr, request_id: requestId,
      body: { operation_id: op === undefined ? "op?" : op, status: "ok", pending: [] },
    });
  }
  async serve(handler) {
    this._handler = handler;
    for (;;) {
      let env;
      try {
        env = await this.framing.read();
      } catch {
        return "eof";
      }
      if (env === null) return "eof";
      if (env.__malformed) continue;
      if (!this._boundOk(env)) continue;
      const body = env.body || {};
      const ty = env.type;
      if (ty === "lifecycle.prepare" || ty === "lifecycle.activate" || ty === "lifecycle.quiesce") {
        await this._replyLifecycle(body.operation_id, env.request_id);
      } else if (ty === "lifecycle.dispose") {
        await this._replyLifecycle(body.operation_id, env.request_id);
        return "dispose";
      } else if (ty === "call.open") {
        const ticket = body.ticket || "";
        const cap = body.capability || "";
        const input = body.input === undefined ? {} : body.input;
        const openRid = env.request_id;
        const ctl = new AbortController();
        this.calls.set(ticket, ctl);
        const ctx = new CallCtx(this, ticket, ctl);
        (async () => {
          let out;
          try {
            out = await handler.onCall(ctx, ticket, cap, input, ctl.signal);
          } catch (e) {
            out = { __error: { code: e.code || "internal", message: e.detail || e.message || String(e) } };
          } finally {
            this.calls.delete(ticket);
          }
          if (ctl.signal.aborted) return; // late after cancel: stay silent
          const rbody = out && out.__error
            ? { ticket, status: "error", error: out.__error }
            : { ticket, status: "ok", output: out === undefined ? null : out };
          try {
            await this._send({
              protocol: PROTOCOL_ID, version: PROTOCOL_VERSION, type: "call.result",
              message_id: `m-call-${ticket}`, session_id: this.sessionId,
              instance_id: this.instanceId, generation: this.generationStr,
              request_id: openRid, body: rbody,
            });
          } catch { /* transport gone */ }
        })();
      } else if (ty === "call.cancel") {
        const ticket = body.ticket || "";
        const ctl = this.calls.get(ticket);
        if (ctl) ctl.abort();
        try {
          handler.onCancel(ticket);
        } catch { /* ignore */ }
      } else if (ty === "dependency.result") {
        if (env.request_id !== undefined) {
          const w = this.depWaiters.get(env.request_id);
          if (w) {
            this.depWaiters.delete(env.request_id);
            if ((body.status || "") === "ok") w.resolve(body.output === undefined ? null : body.output);
            else {
              const e = body.error || {};
              w.reject(new DepError(e.code || "internal", e.message || "remote error"));
            }
          }
        }
      } else if (ty === "resource.result") {
        if (env.request_id !== undefined) {
          const w = this.resWaiters.get(env.request_id);
          if (w) {
            this.resWaiters.delete(env.request_id);
            if ((body.status || "") === "ok") {
              const extra = {};
              for (const k of Object.keys(body))
                if (k !== "operation_id" && k !== "status") extra[k] = body[k];
              w.resolve(extra);
            } else {
              w.reject(new ResError(body.code || "internal", body.message || "remote error"));
            }
          }
        }
      } else if (ty === "event.deliver") {
        const topic = body.topic || "";
        if (topic) this._enqueue({ kind: "event", topic, payload: body.payload });
      } else if (ty === "stream.data") {
        const sid = body.stream_id || "";
        const seqOk = typeof body.seq === "string" && /^[0-9]+$/.test(body.seq);
        if (sid && seqOk)
          this._enqueue({ kind: "stream", streamId: sid, seq: BigInt(body.seq),
                           payload: String(body.payload || "") });
      }
      // other types: ignored without dropping the session
    }
  }
}

module.exports = {
  Component, Handler, CallCtx, SdkError, DepError, ResError,
  PROTOCOL_ID, PROTOCOL_VERSION, DEFAULT_MAX_FRAME, genEq, genParse,
};
