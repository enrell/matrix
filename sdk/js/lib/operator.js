"use strict";
/* Operator/application surface for Node.js (ML1, zero dependencies).
 *
 * Same contract as Python `matrix_operator`: operator calls go through
 * the staged `matrix-managed` binary (`serve` to own a kernel,
 * `request` for authenticated admin actions over mutual TLS). `start`
 * owns its process; `connect` only attaches. Server identity never
 * implies caller authority: `operatorPki` is always explicit.
 */

const { spawn, spawnSync } = require("node:child_process");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const readline = require("node:readline");

const { SdkError } = require("./component");

const EXPECTED_API_PREFIX = "0.1.";
const READY_TIMEOUT_MS = 30000;
const STOP_TIMEOUT_MS = 5000;
const REQUEST_SLACK_S = 10;

class BootstrapError extends SdkError {
  constructor(code, phase, message) {
    super(code, phase, message);
  }
}

class OperatorError extends SdkError {
  constructor(code, message) {
    super(code, "request", message);
  }
}

const KNOWN_CODES = [
  "permission-denied", "stale-generation", "outcome-unknown",
  "unauthenticated", "invalid-message", "unsupported-version",
  "dependency-unavailable", "ambiguous-provider",
  "context-not-active", "resource-exhausted", "deadline-exceeded",
  "cancelled", "cleanup-pending", "internal",
];

function guessCode(stderr) {
  const low = (stderr || "").toLowerCase();
  for (const c of KNOWN_CODES) if (low.includes(c)) return c;
  return (stderr || "").trim() ? "transport" : "internal";
}

function redact(obj) {
  if (Array.isArray(obj)) return obj.map(redact);
  if (obj && typeof obj === "object") {
    const out = {};
    for (const k of Object.keys(obj))
      out[k] = (k === "lease" || k === "launch_token" || k === "token") ? "<redacted>" : redact(obj[k]);
    return out;
  }
  return obj;
}

class Client {
  constructor(binary, listen, ca, cert, key, serverName, owned) {
    this._binary = binary;
    this._listen = listen;
    this._pki = { ca, cert, key };
    this._serverName = serverName || "localhost";
    this._owned = !!owned;
    this._closed = false;
  }
  _checkOpen() {
    if (this._closed) throw new SdkError("internal", "client", "client is closed");
  }
  request(action, timeoutS) {
    this._checkOpen();
    if (!action || typeof action !== "object" || !action.action)
      throw new OperatorError("invalid-message", "action object with 'action' required");
    const t = timeoutS === undefined ? 30 : timeoutS;
    const args = ["request", this._pki.ca, this._pki.cert, this._pki.key,
      this._listen, this._serverName, JSON.stringify(action)];
    let res;
    try {
      res = spawnSync(this._binary, args, { encoding: "utf-8", timeout: (t + REQUEST_SLACK_S) * 1000 });
    } catch (e) {
      if (e.code === "ETIMEDOUT" || (e.message || "").includes("timed out"))
        throw new OperatorError("outcome-unknown", `admin action timed out after ${t}s (not retried)`);
      throw new OperatorError("transport", `spawn: ${e.message}`);
    }
    if (res.error) {
      if (res.error.code === "ETIMEDOUT" || res.error.killed)
        throw new OperatorError("outcome-unknown", `admin action timed out after ${t}s (not retried)`);
      throw new OperatorError("transport", `spawn: ${res.error.message}`);
    }
    if (res.status !== 0) {
      const err = ((res.stderr || "") + (res.stdout || "")).trim();
      throw new OperatorError(guessCode(err), err.split("\n")[0] || "request refused");
    }
    try {
      return JSON.parse(res.stdout);
    } catch (e) {
      throw new OperatorError("internal", `undecodable response: ${e.message}`);
    }
  }
  activate(component, ttlMs, timeoutS) {
    return this.request({ action: "activate", component, ttl_ms: Math.floor(ttlMs === undefined ? 20000 : ttlMs) }, timeoutS);
  }
  status(lease, fence, timeoutS) {
    return this.request({ action: "status", lease, fence: String(fence) }, timeoutS);
  }
  invoke(lease, fence, operation, cap, input, timeoutS) {
    return this.request({ action: "invoke", lease, fence: String(fence), operation, cap, input: input === undefined ? {} : input }, timeoutS);
  }
  release(lease, fence, timeoutS) {
    return this.request({ action: "release", lease, fence: String(fence) }, timeoutS);
  }
  renew(lease, fence, ttlMs, timeoutS) {
    return this.request({ action: "renew", lease, fence: String(fence), ttl_ms: Math.floor(ttlMs === undefined ? 20000 : ttlMs) }, timeoutS);
  }
  async waitReady(lease, fence, timeoutS) {
    const budget = (timeoutS === undefined ? 20 : timeoutS) * 1000;
    const end = Date.now() + budget;
    let last = null;
    while (Date.now() < end) {
      last = this.status(lease, fence, 5);
      if (last && last.ready === true) return last;
      await new Promise((r) => setTimeout(r, 100));
    }
    throw new OperatorError("outcome-unknown",
      `session not ready in budget (last=${JSON.stringify(redact(last))})`);
  }
  /* Marks this handle closed. Never stops any daemon. */
  close() {
    this._closed = true;
  }
}

class OwnedKernel {
  constructor(binary, proc, workdir, client, ready) {
    this._binary = binary;
    this._proc = proc;
    this._workdir = workdir;
    this._client = client;
    this._closed = false;
    this.epoch = ready.epoch;
    this.api = ready.api;
    this.profile = ready.profile;
  }
  get client() {
    return this._client;
  }
  get listen() {
    return this._client._listen;
  }
  async close() {
    if (this._closed) return;
    this._closed = true;
    try {
      this._client.close();
    } finally {
      if (this._proc.exitCode === null) {
        try {
          this._proc.kill("SIGTERM");
          await waitExit(this._proc, STOP_TIMEOUT_MS);
        } catch { /* fall through to kill */ }
        if (this._proc.exitCode === null) {
          try {
            this._proc.kill("SIGKILL");
          } catch { /* gone */ }
          try {
            await waitExit(this._proc, STOP_TIMEOUT_MS);
          } catch { /* give up; OS reaps */ }
        }
      }
      fs.rmSync(this._workdir, { recursive: true, force: true });
    }
  }
}

function waitExit(proc, ms) {
  if (proc.exitCode !== null) return Promise.resolve();
  return new Promise((resolve, reject) => {
    const t = setTimeout(() => {
      proc.removeListener("exit", onExit);
      reject(new Error("stop timeout"));
    }, ms);
    const onExit = () => {
      clearTimeout(t);
      resolve();
    };
    proc.once("exit", onExit);
  });
}

function connect(binary, listen, ca, cert, key, serverName) {
  for (const [label, p] of [["binary", binary], ["ca", ca], ["cert", cert], ["key", key]])
    if (!p || !fs.existsSync(p))
      throw new BootstrapError("transport", "connect", `${label} not found: ${p}`);
  if (!listen) throw new BootstrapError("invalid-message", "connect", "listen address required");
  return new Client(binary, listen, ca, cert, key, serverName, false);
}

async function start(binary, config, operatorPki, serverName) {
  if (!binary) throw new BootstrapError("transport", "spawn", "binary path required");
  try {
    fs.accessSync(binary, fs.constants.X_OK);
  } catch {
    throw new BootstrapError("transport", "spawn", `binary not executable: ${binary}`);
  }
  if (!config || typeof config !== "object" || !config.home)
    throw new BootstrapError("invalid-message", "config", "config object with 'home' required");
  const workdir = fs.mkdtempSync(path.join(os.tmpdir(), "mx-js-"));
  let proc = null;
  try {
    const cfgPath = path.join(workdir, "config.json");
    fs.writeFileSync(cfgPath, JSON.stringify(config));
    proc = spawn(binary, ["serve", cfgPath], { stdio: ["ignore", "pipe", "pipe"] });
    const ready = await readReady(proc);
    const listen = ready.listen || ((config.tls || {}).listen || "");
    if (!operatorPki || !operatorPki.ca || !operatorPki.cert || !operatorPki.key)
      throw new BootstrapError("invalid-message", "config",
        "operator PKI (ca/cert/key) is required: server identity never implies caller authority");
    for (const k of ["ca", "cert", "key"])
      if (!fs.existsSync(operatorPki[k]))
        throw new BootstrapError("invalid-message", "config", `operator ${k} not found: ${operatorPki[k]}`);
    const client = new Client(binary, listen, operatorPki.ca, operatorPki.cert,
      operatorPki.key, serverName, true);
    return new OwnedKernel(binary, proc, workdir, client, ready);
  } catch (e) {
    if (proc && proc.exitCode === null) {
      try {
        proc.kill("SIGKILL");
      } catch { /* gone */ }
    }
    fs.rmSync(workdir, { recursive: true, force: true });
    throw e;
  }
}

function readReady(proc) {
  return new Promise((resolve, reject) => {
    const rl = readline.createInterface({ input: proc.stdout });
    const timer = setTimeout(() => {
      rl.close();
      reject(new BootstrapError("transport", "ready", "no ready line in budget"));
    }, READY_TIMEOUT_MS);
    let stderrTail = "";
    if (proc.stderr) {
      proc.stderr.setEncoding("utf-8");
      proc.stderr.on("data", (d) => {
        stderrTail += d;
        if (stderrTail.length > 2000) stderrTail = stderrTail.slice(-2000);
      });
    }
    proc.once("exit", (code) => {
      clearTimeout(timer);
      rl.close();
      const first = stderrTail.trim().split("\n")[0] || `exit ${code}`;
      reject(new BootstrapError("internal", "config", `daemon refused config: ${first}`));
    });
    rl.once("line", (line) => {
      clearTimeout(timer);
      rl.close();
      let ready;
      try {
        ready = JSON.parse(line);
      } catch {
        reject(new BootstrapError("transport", "ready", `undecodable ready line: ${line.slice(0, 120)}`));
        return;
      }
      if (!ready || ready.ready !== true) {
        reject(new BootstrapError("transport", "ready", `daemon not ready: ${line.slice(0, 160)}`));
        return;
      }
      const api = String(ready.api || "");
      if (api && !api.startsWith(EXPECTED_API_PREFIX)) {
        reject(new BootstrapError("unsupported-version", "version",
          `binary api ${JSON.stringify(api)} outside ${EXPECTED_API_PREFIX}x`));
        return;
      }
      resolve(ready);
    });
  });
}

async function doctor(binary) {
  const report = {
    node: process.version,
    binary: binary || "",
    binaryFound: false,
    binaryExecutable: false,
    cliShapeOk: false,
    openssl: false,
    bwrap: false,
    socketDirWritable: false,
    errors: [],
  };
  const pathEnv = (process.env.PATH || "").split(path.delimiter);
  const which = (n) => pathEnv.some((d) => {
    try {
      return fs.existsSync(path.join(d, n));
    } catch {
      return false;
    }
  });
  report.openssl = which("openssl");
  report.bwrap = which("bwrap");
  if (binary && fs.existsSync(binary)) {
    report.binaryFound = true;
    try {
      fs.accessSync(binary, fs.constants.X_OK);
      report.binaryExecutable = true;
      const res = spawnSync(binary, [], { encoding: "utf-8", timeout: 10000 });
      const out = (res.stderr || "") + (res.stdout || "");
      if (out.includes("matrix-managed serve")) report.cliShapeOk = true;
      else report.errors.push("binary does not speak the managed CLI shape");
    } catch (e) {
      report.errors.push(`binary probe failed: ${e.message}`);
    }
    if (!report.binaryExecutable) report.errors.push("binary not executable");
  } else {
    report.errors.push("binary not found: set it explicitly or via PATH (no silent download)");
  }
  try {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "mx-doc-"));
    const net = require("node:net");
    await new Promise((resolve, reject) => {
      const srv = net.createServer();
      srv.on("error", reject);
      srv.listen(path.join(dir, "t.sock"), () => {
        report.socketDirWritable = true;
        srv.close(() => resolve());
      });
    });
    fs.rmSync(dir, { recursive: true, force: true });
  } catch (e) {
    report.errors.push(`unix socket probe failed: ${e.message}`);
  }
  return report;
}

module.exports = {
  Client, OwnedKernel, BootstrapError, OperatorError,
  connect, start, doctor, redact,
};
