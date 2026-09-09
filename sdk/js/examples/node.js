"use strict";
/* Generic Matrix test node (ML1 contract: docs/ML1-NODE.md).
 * Usage: node node.js --matrix-sock <sock> --id <logical>
 *        [--event-log <path>] [--stream-log <path>] [--stream-slow-ms <n>]
 */
const fs = require("node:fs");
const { Component, Handler } = require("../lib/component");

function appendLine(p, line) {
  try {
    fs.appendFileSync(p, line + "\n");
  } catch { /* log loss never fails the call */ }
}

function sleepMs(ms, signal) {
  return new Promise((resolve, reject) => {
    if (signal.aborted) return reject(Object.assign(new Error("aborted"), { code: "cancelled" }));
    const t = setTimeout(() => {
      signal.removeEventListener("abort", onAbort);
      resolve(false);
    }, ms);
    const onAbort = () => {
      clearTimeout(t);
      reject(Object.assign(new Error("aborted"), { code: "cancelled" }));
    };
    signal.addEventListener("abort", onAbort, { once: true });
  });
}

async function abortableSleep(totalMs, signal) {
  let slept = 0;
  while (slept < totalMs) {
    const step = Math.min(5, totalMs - slept);
    await sleepMs(step, signal);
    slept += step;
  }
}

class Node extends Handler {
  constructor(id, eventLog, streamLog, streamSlowMs) {
    super();
    this.id = id;
    this.eventLog = eventLog;
    this.streamLog = streamLog;
    this.streamSlowMs = streamSlowMs;
  }
  onEvent(topic, payload) {
    if (this.eventLog) appendLine(this.eventLog, `${topic}\t${JSON.stringify(payload)}`);
  }
  async onStream(streamId, seq, payload) {
    if (this.streamSlowMs > 0) await new Promise((r) => setTimeout(r, this.streamSlowMs));
    if (this.streamLog) appendLine(this.streamLog, `${streamId}\t${seq}\t${payload.length}`);
  }
  async onCall(ctx, ticket, cap, input, signal) {
    input = input && typeof input === "object" ? input : {};
    const sleepN = Number(input.sleep_ms || 0);
    if (sleepN > 0) {
      try {
        await abortableSleep(sleepN, signal);
      } catch (e) {
        throw Object.assign(new Error("aborted"), { code: "cancelled" });
      }
    }
    if (typeof input.fail === "string" && input.fail)
      throw Object.assign(new Error(`remote ${input.fail}`), { code: input.fail });
    if (Number.isInteger(input.amplify)) {
      const n = Math.min(Math.max(input.amplify, 0), 1 << 20);
      return { blob: "x".repeat(n), via: this.id };
    }
    if (input.chain) {
      const bindings = ctx.dependencies();
      if (!bindings.length) throw Object.assign(new Error("no binding"), { code: "dependency-unavailable" });
      const inner = input.input && typeof input.input === "object" ? input.input : {};
      const rawTimeout = Number(input.timeout_ms ?? 5000);
      const timeoutMs = Math.max(Number.isFinite(rawTimeout) ? rawTimeout : 5000, 1);
      const out = await ctx.invokeDependency(bindings[0].id, inner, timeoutMs / 1000);
      return { chained: out, via: this.id };
    }
    if (input.acquire && typeof input.acquire === "object") {
      const acq = input.acquire;
      const ms = acq.interval_ms === undefined || acq.interval_ms === null ? undefined : Number(acq.interval_ms);
      const h = await ctx.acquireResource(String(acq.kind || ""), String(acq.label || ""), ms);
      return { acquired: { handle: h.toString() }, via: this.id };
    }
    const rel = input.release;
    if ((typeof rel === "number" && Number.isInteger(rel) && rel >= 0) ||
        (typeof rel === "bigint" && rel >= 0n)) {
      await ctx.releaseResource(rel);
      return { released: String(rel), via: this.id };
    }
    if (input.stream_send && typeof input.stream_send === "object") {
      const spec = input.stream_send;
      const streamId = String(spec.stream_id || "s-test");
      const chunks = Math.min(Math.max(Number(spec.chunks || 0), 0), 256);
      const nbytes = Math.min(Math.max(Number(spec.chunk_bytes || 0), 0), 4096);
      const slp = Math.max(Number(spec.sleep_ms || 0), 0);
      const payload = "x".repeat(nbytes);
      let sent = 0;
      for (let seq = 0; seq < chunks; seq++) {
        if (signal.aborted) throw Object.assign(new Error("aborted"), { code: "cancelled" });
        try {
          await ctx.sendStream(streamId, seq, payload);
        } catch (e) {
          throw Object.assign(new Error(String(e.detail || e.message)), { code: "stream-refused" });
        }
        sent++;
        if (slp > 0) await abortableSleep(Math.min(slp, 50), signal).catch(() => {
          throw Object.assign(new Error("aborted"), { code: "cancelled" });
        });
      }
      return { stream_sent: sent, via: this.id };
    }
    if (input.chain_with_streams && typeof input.chain_with_streams === "object") {
      // Concurrent chain + streams (M7 bidi legs): streams while the
      // child leg is in flight on this same session.
      const spec = input.chain_with_streams;
      const streamId = String(spec.stream_id || "s-bidi");
      const chunks = Math.min(Math.max(Number(spec.chunks || 0), 0), 32);
      const nbytes = Math.min(Math.max(Number(spec.chunk_bytes || 0), 0), 1024);
      const interval = Math.min(Math.max(Number(spec.interval_ms ?? 20), 0), 50);
      const prime = Math.min(Math.max(Number(spec.prime_ms ?? 50), 0), 1000);
      const payload = "x".repeat(nbytes);
      let sent = 0;
      const streamer = (async () => {
        if (prime > 0) await new Promise((r) => setTimeout(r, prime));
        for (let seq = 0; seq < chunks; seq++) {
          try {
            await ctx.sendStream(streamId, seq, payload);
            sent++;
          } catch {
            break;
          }
          if (interval > 0) await new Promise((r) => setTimeout(r, interval));
        }
      })();
      const bindings = ctx.dependencies();
      if (!bindings.length) {
        await streamer;
        throw Object.assign(new Error("no binding"), { code: "dependency-unavailable" });
      }
      const inner = spec.input && typeof spec.input === "object" ? spec.input : {};
      const rawTimeout = Number(spec.timeout_ms ?? 8000);
      const timeoutMs = Math.max(Number.isFinite(rawTimeout) ? rawTimeout : 8000, 1);
      let chainedOut;
      try {
        chainedOut = await ctx.invokeDependency(bindings[0].id, inner, timeoutMs / 1000);
      } catch (e) {
        await streamer;
        throw Object.assign(new Error(String(e.detail || e.message)), { code: e.code || "internal" });
      }
      await streamer;
      return { chained: chainedOut, via: this.id, stream_sent: sent };
    }
    return { echo: input, via: this.id };
  }
}

async function main() {
  const args = process.argv.slice(2);
  const get = (k) => {
    const i = args.indexOf(k);
    return i >= 0 ? args[i + 1] : undefined;
  };
  const sock = get("--matrix-sock");
  const id = get("--id") || "dep-node";
  if (!sock) {
    process.stderr.write("usage: node.js --matrix-sock <sock> [--id <logical>] ...\n");
    process.exit(2);
  }
  let comp;
  try {
    comp = await Component.connect(sock, id);
  } catch (e) {
    process.stderr.write(`connect: ${e.message}\n`);
    process.exit(2);
  }
  const reason = await comp.serve(new Node(id, get("--event-log") || null,
    get("--stream-log") || null, Number(get("--stream-slow-ms") || 0)));
  if (reason !== "dispose" && reason !== "eof") {
    process.stderr.write(`serve: ${reason}\n`);
    process.exit(1);
  }
}

if (require.main === module) main().catch((e) => {
  process.stderr.write(`fatal: ${e.stack || e}\n`);
  process.exit(1);
});
module.exports = { Node };
