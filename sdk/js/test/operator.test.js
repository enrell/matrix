"use strict";
/* Operator-surface tests: mapping over a fake binary, bootstrap
 * phases, doctor shape; live parts need MX_MATRIX_MANAGED + openssl. */
const { describe, it } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const { execFileSync } = require("node:child_process");
const { Client, BootstrapError, OperatorError, connect, start, doctor } = require("../lib/operator");

const HERE = path.dirname(__filename);
const ROOT = path.dirname(path.dirname(path.dirname(HERE)));
const BIN = process.env.MX_MATRIX_MANAGED ||
  path.join(ROOT, "target", "release", "matrix-managed");
const DEV_PKI = path.join(ROOT, "scripts", "dev-pki.py");
const HAVE_BIN = fs.existsSync(BIN);
const HAVE_OPENSSL = (process.env.PATH || "").split(path.delimiter)
  .some((d) => { try { return fs.existsSync(path.join(d, "openssl")); } catch { return false; } });
const LIVE = HAVE_BIN && HAVE_OPENSSL && fs.existsSync(DEV_PKI);

function writeFakeBinary() {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "opfake-"));
  const fake = path.join(dir, "matrix-managed");
  fs.writeFileSync(fake, `#!/bin/sh
if [ "$1" = "request" ]; then
  case "$7" in
    *sleep*) exec sleep 30;;
    *badjson*) echo 'not json';;
    *denied*) echo 'permission-denied: nope' >&2; exit 1;;
    *) echo '{"ok":true}';;
  esac
else echo 'usage: matrix-managed serve <config>' >&2; exit 1
fi
`);
  fs.chmodSync(fake, 0o755);
  for (const n of ["ca", "cert", "key"]) fs.writeFileSync(path.join(dir, n), "");
  return dir;
}

describe("operator mapping", () => {
  it("maps CLI outcomes to typed errors", () => {
    const dir = writeFakeBinary();
    try {
      const fake = path.join(dir, "matrix-managed");
      const c = new Client(fake, "127.0.0.1:9", path.join(dir, "ca"),
        path.join(dir, "cert"), path.join(dir, "key"));
      assert.deepEqual(c.request({ action: "ping" }), { ok: true });
      assert.throws(() => c.request({ action: "denied-op" }),
        (e) => e instanceof OperatorError && e.code === "permission-denied");
      assert.throws(() => c.request({ action: "badjson" }),
        (e) => e instanceof OperatorError && e.code === "internal");
      assert.throws(() => c.request({ action: "sleep" }, 1),
        (e) => e instanceof OperatorError && e.code === "outcome-unknown");
      assert.throws(() => c.request({ "no-action": true }),
        (e) => e instanceof OperatorError && e.code === "invalid-message");
      c.close();
      assert.throws(() => c.request({ action: "ping" }), /closed/);
    } finally {
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it("bootstrap phases are explicit", async () => {
    await assert.rejects(start("/nonexistent/matrix-managed", { home: "/tmp/x" }),
      (e) => e instanceof BootstrapError && e.phase === "spawn");
    await assert.rejects(start(process.execPath, { components: [] }),
      (e) => e instanceof BootstrapError && e.phase === "config");
    assert.throws(() => connect("/nonexistent/x", "127.0.0.1:1", "a", "b", "c"),
      (e) => e instanceof BootstrapError);
  });

  it("doctor shape carries no secrets", async () => {
    const rep = await doctor("/nonexistent/binary");
    for (const k of ["node", "binary", "binaryFound", "cliShapeOk",
      "openssl", "bwrap", "socketDirWritable", "errors"])
      assert.ok(k in rep, k);
    assert.ok(!JSON.stringify(rep).replace("socketDirWritable", "").includes("lease"));
    if (HAVE_BIN) {
      const rep2 = await doctor(BIN);
      assert.ok(rep2.binaryFound && rep2.cliShapeOk, JSON.stringify(rep2));
    }
  });
});

describe("operator live", { skip: !LIVE }, () => {
  it("start/attach lifecycle with stable denial codes", async () => {
    const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "oplive-"));
    try {
      const pki = path.join(tmp, "pki");
      execFileSync("python3", [DEV_PKI, pki, "--server-name", "localhost"], { stdio: "pipe" });
      const fp = execFileSync("python3",
        ["-c", "import hashlib,sys; print(hashlib.sha256(open(sys.argv[1],'rb').read()).hexdigest())",
          path.join(pki, "client.der")], { encoding: "utf-8" }).trim();
      const cfg = {
        home: path.join(tmp, "home"),
        components: [{ manifest: { id: "echo", capabilities: ["echo.msg@1"], reducer: "echo" }, trusted: true }],
        grants: { [fp]: { components: ["echo"], capabilities: ["echo.msg@1"] } },
        tls: {
          listen: "127.0.0.1:0", ca: path.join(pki, "ca.der"),
          cert: path.join(pki, "server.der"), key: path.join(pki, "server-key.der"),
        },
      };
      const opki = {
        ca: path.join(pki, "ca.der"), cert: path.join(pki, "client.der"),
        key: path.join(pki, "client-key.der"),
      };
      const kernel = await start(BIN, cfg, opki);
      try {
        assert.ok(kernel.api.startsWith("0.1."), kernel.api);
        const act = kernel.client.activate("echo", 20000);
        const v = kernel.client.invoke(act.lease, act.fence, "op-live-1", "echo.msg@1", { ping: 1 });
        assert.equal(v.ok, true);
        const attached = connect(BIN, kernel.listen, opki.ca, opki.cert, opki.key);
        const v2 = attached.invoke(act.lease, act.fence, "op-live-2", "echo.msg@1", {});
        assert.equal(v2.ok, true);
        attached.close(); // attachment owns nothing: daemon keeps serving
        const v3 = kernel.client.invoke(act.lease, act.fence, "op-live-3", "echo.msg@1", {});
        assert.equal(v3.ok, true);
        assert.throws(() => kernel.client.invoke("dead", "1", "op-x", "echo.msg@1", {}),
          (e) => e instanceof OperatorError);
        kernel.client.release(act.lease, act.fence);
      } finally {
        await kernel.close();
      }
      assert.throws(() => kernel.client.invoke("x", "1", "op-x", "echo.msg@1", {}), /closed/);
    } finally {
      fs.rmSync(tmp, { recursive: true, force: true });
    }
  });
});
