# Conformance suite
Purpose: prove every mx-node language behaves identically on matrix-managed.
Reuses staged mx-node binaries + staged matrix-managed from dist/.
No in-tree paths at runtime; artifacts staged under HARNESS_DIR.
Specs live in spec/cNN-*.yaml (GIVEN/WHEN/THEN + INVARIANTS).
Harness in harness/run.sh executes one spec against one chain.
Fixtures in fixtures/ are thin adapters; logic lives in mx-node.
Run: scripts/package.sh && tests/conformance/harness/run.sh spec/c01-smoke.yaml
Pass = ok true + zero orphan leases/socks/pids + no secrets in logs.
Full scenarios (L02+) land after c01-smoke goes green.
