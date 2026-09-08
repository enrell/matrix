# matrix-component (Node.js SDK)

Experimental Matrix SDK for Node.js (ML1 contract surface, version
`0.1.0`). Local validation artifact only — not for registry
publication (the repository ships no license file).

Contract reference: `docs/SDK.md`, `docs/ML1-NODE.md`,
`docs/ML1-MATRIX.md` in the Matrix repository. Same observable
behavior as the reference SDKs (Rust `matrix-component`, Python
`matrix_component`).

## Install

```sh
npm install --offline --no-audit --no-fund /path/to/matrix-component-*.tgz
```

Zero dependencies. Node >= 20. Plain JS needs no compiler;
`index.d.ts` provides types for TypeScript users (checked with
TypeScript 5.6, `--strict --target es2022`; JS and typed TS count as
distinct supported entries over this same SDK).

## Component side

```js
const { Component, Handler } = require("matrix-component");

class Echo extends Handler {
  async onCall(ctx, ticket, cap, input, signal) {
    if (input.chain) {
      const out = await ctx.invokeDependency(
        ctx.dependencies()[0].id, input.input, 5);
      return { chained: out, via: "echo" };
    }
    return { echo: input, via: "echo" };
  }
  onEvent(topic, payload) { /* observe fast, never block */ }
  onStream(streamId, seq, payload) { /* seq is a BigInt */ }
}

const comp = await Component.connect(process.env.MATRIX_SOCK, "echo");
await comp.serve(new Echo()); // "dispose" | "eof"
```

- One `onCall` task per call; cancel arrives as `AbortSignal`
  (`signal.aborted`) plus `onCancel(ticket)`. Late answers after
  cancel stay silent.
- `sendStream(streamId, seq, text)`: text only — binary input is
  refused (`invalid-message`), never lossy-converted.
- `u64` wire values (generation, seq, handles) are decimal strings;
  the SDK compares them as `BigInt` (no precision loss).
- Events/streams share a bounded edge queue (64, drop-oldest,
  counted in `ctx.eventDroppedCount()`); observers may be async —
  slowness throttles the sender via host credit, never blocks the
  reader.
- Without a negotiated `dependency-calls/1`,
  `invokeDependency` throws `DepError("unsupported-feature")`
  without touching the wire.

## Operator side

```js
const { start, connect } = require("matrix-component");
const kernel = await start("/path/to/matrix-managed", config, operatorPki);
const act = kernel.client.activate("prov", 30000);
const v = kernel.client.invoke(act.lease, act.fence, "op-1",
  "prov.echo@1", { ping: 1 });
await kernel.close(); // owned: reaps only this daemon
```

Operator calls travel through the staged `matrix-managed` binary
(`serve`/`request`, mutual TLS). `start` owns its daemon;
`connect` attaches (closing never stops a shared kernel).
`operatorPki` is always explicit: server identity never implies
caller authority. Timeouts report `outcome-unknown` and never
retry. Error codes (`permission-denied`, `stale-generation`, …)
surface as `OperatorError.code`.

## Scaffold, doctor, tests

```sh
npx matrix-scaffold myapp ./myapp --pki ./pki --home ./priv-home
npx matrix-doctor --binary /path/to/matrix-managed
npm test  # node --test; live parts need MX_MATRIX_MANAGED + openssl
```

`matrix-doctor` prints environment diagnosis as JSON (binary shape,
versions, socket permissions, isolation prerequisites) and redacts
secrets. Scaffolded projects are covered by `templates/README.md`
(build/install/config/run/test/cleanup).
