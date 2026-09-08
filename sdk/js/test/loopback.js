"use strict";
/* Shared loopback fake host for the JS SDK tests (stdlib only). */
const net = require("node:net");
const os = require("node:os");
const path = require("node:path");
const fs = require("node:fs");

const MAX_FRAME = 1024 * 1024;

function send(sock, msg) {
  const raw = Buffer.from(JSON.stringify(msg), "utf-8");
  const frame = Buffer.alloc(4 + raw.length);
  frame.writeUInt32BE(raw.length, 0);
  raw.copy(frame, 4);
  return new Promise((resolve, reject) => sock.write(frame, (e) => (e ? reject(e) : resolve())));
}

function read(sock, timeoutMs = 10000) {
  return new Promise((resolve, reject) => {
    let buf = Buffer.alloc(0);
    const timer = setTimeout(() => {
      cleanup();
      reject(new Error("read timeout"));
    }, timeoutMs);
    const onData = (d) => {
      buf = Buffer.concat([buf, d]);
      if (buf.length >= 4) {
        const n = buf.readUInt32BE(0);
        if (buf.length >= 4 + n) {
          cleanup();
          try {
            resolve(JSON.parse(buf.subarray(4, 4 + n).toString("utf-8")));
          } catch (e) {
            reject(e);
          }
        }
      }
    };
    const onClose = () => {
      cleanup();
      reject(new Error("eof"));
    };
    const cleanup = () => {
      clearTimeout(timer);
      sock.removeListener("data", onData);
      sock.removeListener("close", onClose);
    };
    sock.on("data", onData);
    sock.on("close", onClose);
  });
}

function env(ty, body, rid = "r1") {
  return {
    protocol: "matrix.component", version: "0.1", type: ty,
    message_id: "m1", session_id: "s1", instance_id: "1",
    generation: "1", request_id: rid, body,
  };
}

function callOpen(ticket, input) {
  return {
    protocol: "matrix.component", version: "0.1", type: "call.open",
    message_id: `m-${ticket}`, session_id: "s1", instance_id: "1",
    generation: "1", request_id: `r-${ticket}`,
    body: { ticket, capability: "c@1", input },
  };
}

/* Starts a fake host; `script(conn)` drives the conversation.
 * Resolves {sockPath, done} where done settles when script finishes. */
async function loopback({ features, bindings }, script) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "jsunits-"));
  const sockPath = path.join(dir, "t.sock");
  const srv = net.createServer();
  await new Promise((resolve) => srv.listen(sockPath, resolve));
  const done = (async () => {
    const conn = await new Promise((resolve) => srv.once("connection", resolve));
    try {
      const hello = await read(conn);
      if (hello.type !== "hello") throw new Error(`expected hello: ${JSON.stringify(hello)}`);
      await send(conn, {
        protocol: "matrix.component", version: "0.1", type: "welcome",
        message_id: "h1", session_id: "s1",
        body: { version: "0.1", max_frame: MAX_FRAME, limits: {}, features },
      });
      const reg = await read(conn);
      if (reg.type !== "component.register") throw new Error("expected register");
      await send(conn, {
        protocol: "matrix.component", version: "0.1", type: "registered",
        message_id: "r", session_id: "s1", instance_id: "1",
        generation: "1", body: { logical: "t" },
      });
      const act = env("lifecycle.activate",
        { operation_id: "op", manifest: {}, bindings: [], dependency_bindings: bindings }, "q");
      await send(conn, act);
      const lc = await read(conn);
      if (lc.type !== "lifecycle.result") throw new Error("expected lifecycle.result");
      await script(conn);
    } finally {
      conn.destroy();
      srv.close();
      fs.rmSync(dir, { recursive: true, force: true });
    }
  })();
  return { sockPath, done };
}

function dispose() {
  return {
    protocol: "matrix.component", version: "0.1", type: "lifecycle.dispose",
    message_id: "d1", session_id: "s1", instance_id: "1",
    generation: "1", request_id: "rd1",
    body: { operation_id: "op", deadline_ms: 100 },
  };
}

module.exports = { send, read, env, callOpen, dispose, loopback, MAX_FRAME };
