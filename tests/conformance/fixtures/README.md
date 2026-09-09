# Fixtures: thin per-language adapters
Adapters only map spec steps to mx-node CLI/config; no business logic here.
Business logic lives in mx-node per language (echo/chain/acquire/release).
Prefer shared helpers in harness/run.sh over per-language scripts.
New language = new adapter + entrypoint export, no spec changes.
