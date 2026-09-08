#!/usr/bin/env node
"use strict";
/* matrix-scaffold: materialize a Matrix JS node project (ML1). No network.
 * Usage: matrix-scaffold <name> <dir> [--pki <dir>] [--home <dir>] */
const fs = require("node:fs");
const path = require("node:path");
const { execFileSync } = require("node:child_process");

function fail(msg) {
  process.stderr.write(msg + "\n");
  process.exit(2);
}
const args = process.argv.slice(2);
if (args.length < 2 || args.includes("-h") || args.includes("--help"))
  fail("usage: matrix-scaffold <name> <dir> [--pki <dir>] [--home <dir>]");
const [name, dir] = args;
let pki = "@PKI@";
let home = "@HOME@";
for (let i = 2; i < args.length; i++) {
  if (args[i] === "--pki") pki = args[++i];
  else if (args[i] === "--home") home = args[++i];
  else fail(`unknown option: ${args[i]}`);
}
const root = path.dirname(path.dirname(fs.realpathSync(__filename)));
let fp = "@FINGERPRINT@";
if (fs.existsSync(path.join(pki, "client.der"))) {
  try {
    const out = execFileSync("python3",
      ["-c", "import hashlib,sys; print(hashlib.sha256(open(sys.argv[1],'rb').read()).hexdigest())",
        path.join(pki, "client.der")], { encoding: "utf-8" }).trim();
    if (/^[0-9a-f]{64}$/.test(out)) fp = out;
  } catch { /* keep placeholder */ }
}
// Self-contained: vendor the SDK lib next to the node (no npm at run).
for (const marker of ["scaffold.sh", "package.json"])
  if (fs.existsSync(path.join(dir, marker))) {
    process.stderr.write(`refusing to scaffold onto SDK sources: ${dir}\n`);
    process.exit(2);
  }
fs.mkdirSync(path.join(dir, "mx"), { recursive: true });
for (const f of ["component.js", "operator.js", "index.js"])
  fs.copyFileSync(path.join(root, "lib", f), path.join(dir, "mx", f));
let nodeSrc = fs.readFileSync(path.join(root, "examples", "node.js"), "utf-8");
nodeSrc = nodeSrc.replaceAll('require("../lib/component")', 'require("./mx/component")');
fs.writeFileSync(path.join(dir, "node.js"), nodeSrc);
const sub = (s) => s
  .replaceAll("@NODEBIN@", process.execPath)
  .replaceAll("@DIR@", dir)
  .replaceAll("@PKI@", pki)
  .replaceAll("@HOME@", home)
  .replaceAll("@FINGERPRINT@", fp)
  .replaceAll("<NAME>", name);
for (const f of ["config.json", "README.md"])
  fs.writeFileSync(path.join(dir, f), sub(fs.readFileSync(path.join(root, "templates", f), "utf-8")));
JSON.parse(fs.readFileSync(path.join(dir, "config.json"), "utf-8"));
console.log(`scaffolded ${name} at ${dir} (fingerprint: ${fp})`);
