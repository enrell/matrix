"use strict";
const { describe, it } = require("node:test");
const assert = require("node:assert/strict");
const { Component, Handler, genEq, genParse } = require("../lib/component");
const { send, read, callOpen, dispose, loopback } = require("./loopback");

describe("u64 handling", () => {
  it("keeps full precision as decimal strings", () => {
    assert.equal(genParse("18446744073709551615").toString(), "18446744073709551615");
    assert.ok(genEq("18446744073709551615", "18446744073709551615"));
    assert.ok(!genEq("18446744073709551615", "18446744073709551614"));
    assert.ok(!genEq("nope", "1"));
  });
});

describe("framing robustness", () => {
  it("malformed frame drops silently, session survives", async () => {
    const { sockPath, done } = await loopback({ features: [], bindings: [] }, async (conn) => {
      // raw garbage frame (valid length, invalid JSON)
      const garbage = Buffer.from("{oops", "utf-8");
      const frame = Buffer.alloc(4 + garbage.length);
      frame.writeUInt32BE(garbage.length, 0);
      garbage.copy(frame, 4);
      await new Promise((res, rej) => conn.write(frame, (e) => (e ? rej(e) : res())));
      await send(conn, callOpen("tkt-9", { ping: 1 }));
      const ans = await read(conn);
      assert.equal(ans.type, "call.result");
      assert.deepEqual(ans.body.output, { echo: { ping: 1 } });
      await send(conn, dispose());
      await read(conn);
    });
    class Echo extends Handler {
      async onCall(ctx, ticket, cap, input) {
        return { echo: input };
      }
    }
    const comp = await Component.connect(sockPath, "t");
    const rc = await comp.serve(new Echo());
    await done;
    assert.equal(rc, "dispose");
  });

  it("stale generation ignored, current still served", async () => {
    const { sockPath, done } = await loopback({ features: [], bindings: [] }, async (conn) => {
      const stale = callOpen("tkt-stale", {});
      stale.generation = "999";
      await send(conn, stale);
      await send(conn, callOpen("tkt-9", {}));
      const ans = await read(conn);
      assert.equal(ans.body.ticket, "tkt-9");
      await send(conn, dispose());
      await read(conn);
    });
    class Echo extends Handler {
      async onCall(ctx, ticket) {
        return { ticket };
      }
    }
    const comp = await Component.connect(sockPath, "t");
    const rc = await comp.serve(new Echo());
    await done;
    assert.equal(rc, "dispose");
  });
});

describe("feature gate", () => {
  it("invoke without negotiation refuses locally, wire untouched", async () => {
    let extra = "unset";
    const { sockPath, done } = await loopback({ features: [], bindings: [] }, async (conn) => {
      await send(conn, callOpen("tkt-9", {}));
      const ans = await read(conn);
      assert.deepEqual(ans.body.output, { refused: "unsupported-feature" });
      try {
        extra = await read(conn, 1000);
      } catch (e) {
        extra = `quiet:${e.message}`;
      }
      await send(conn, dispose());
      await read(conn);
    });
    class Gate extends Handler {
      async onCall(ctx) {
        try {
          await ctx.invokeDependency("bind-x", {}, 2);
          return { unexpected: "wire-touched" };
        } catch (e) {
          return { refused: e.code };
        }
      }
    }
    const comp = await Component.connect(sockPath, "t");
    assert.ok(!comp.features.includes("dependency-calls/1"));
    const rc = await comp.serve(new Gate());
    await done;
    assert.equal(rc, "dispose");
    assert.match(String(extra), /^quiet:/);
  });
});

describe("dependency roundtrip", () => {
  it("open/result correlate by request id", async () => {
    const { sockPath, done } = await loopback(
      { features: ["dependency-calls/1"], bindings: [{ binding_id: "bind-1", capability: "c@1" }] },
      async (conn) => {
        await send(conn, callOpen("tkt-9", { chain_it: true }));
        const opened = await read(conn);
        assert.equal(opened.type, "dependency.open");
        assert.equal(opened.body.binding_id, "bind-1");
        assert.equal(opened.body.parent_ticket, "tkt-9");
        await send(conn, {
          protocol: "matrix.component", version: "0.1", type: "dependency.result",
          message_id: "mres", session_id: "s1", instance_id: "1",
          generation: "1", request_id: opened.request_id,
          body: { status: "ok", output: { deep: 1 } },
        });
        const ans = await read(conn);
        assert.deepEqual(ans.body.output, { got: { deep: 1 } });
        await send(conn, dispose());
        await read(conn);
      });
    class Chain extends Handler {
      async onCall(ctx, ticket, cap, input) {
        if (input.chain_it) {
          const out = await ctx.invokeDependency(ctx.dependencies()[0].id, { v: 1 }, 5);
          return { got: out };
        }
        return {};
      }
    }
    const comp = await Component.connect(sockPath, "t");
    const rc = await comp.serve(new Chain());
    await done;
    assert.equal(rc, "dispose");
  });
});

describe("reader independence", () => {
  it("slow onEvent does not stall calls; flood drops are counted", async () => {
    const events = [];
    const { sockPath, done } = await loopback({ features: [], bindings: [] }, async (conn) => {
      const { env } = require("./loopback");
      for (let i = 0; i < 120; i++)
        await send(conn, env("event.deliver", { topic: "t", payload: { n: i } }));
      const t0 = Date.now();
      await send(conn, callOpen("tkt-9", {}));
      const ans = await read(conn);
      const dt = Date.now() - t0;
      assert.equal(ans.type, "call.result");
      assert.ok(dt < 5000, `call answered under flood in ${dt}ms`);
      await send(conn, callOpen("tkt-10", { report_drops: true }));
      const rep = await read(conn);
      assert.ok(rep.body.output.dropped >= 1, JSON.stringify(rep.body.output));
      await send(conn, dispose());
      await read(conn);
    });
    class Slow extends Handler {
      async onCall(ctx, ticket, cap, input) {
        if (input.report_drops) return { dropped: ctx.eventDroppedCount() };
        return { ok: true };
      }
      async onEvent(topic, payload) {
        // slow observer yields: the reader keeps flowing while we lag,
        // so the bounded queue overflows and counts the loss.
        await new Promise((r) => setTimeout(r, 30));
        events.push(payload);
      }
    }
    const comp = await Component.connect(sockPath, "t");
    const rc = await comp.serve(new Slow());
    await done;
    assert.equal(rc, "dispose");
  });
});

describe("binary refusal", () => {
  it("non-text stream payload refused, never converted", async () => {
    const { sockPath, done } = await loopback({ features: [], bindings: [] }, async (conn) => {
      await send(conn, callOpen("tkt-9", { send_bytes: true }));
      const ans = await read(conn);
      assert.equal(ans.body.status, "error");
      assert.equal(ans.body.error.code, "invalid-message");
      await send(conn, dispose());
      await read(conn);
    });
    class Bin extends Handler {
      async onCall(ctx, ticket, cap, input) {
        if (input.send_bytes) await ctx.sendStream("s1", 0, Buffer.from([0xff, 0xfe]));
        return {};
      }
    }
    const comp = await Component.connect(sockPath, "t");
    const rc = await comp.serve(new Bin());
    await done;
    assert.equal(rc, "dispose");
  });
});
