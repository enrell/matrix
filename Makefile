BIN := target/release/matrix-rt
HOME_DIR := $(CURDIR)

.PHONY: build test compat clean run status

build:
	cargo build --release

test:
	cargo build --release -p matrix-host --examples
	cargo test --release -- --test-threads=1
	python3 sdk-python/test_units.py

compat: build
	@echo "-- ancient frozen (hash vivo, sem binário externo)"
	@sha256sum plugins/ancient.json | cut -d' ' -f1 | grep -q "$$(cat plugins/ancient.sha256)" && echo "ancient manifest OK"
	@rm -rf run/compat-check; mkdir -p run/compat-check/plugins
	@cp plugins/*.json run/compat-check/plugins/
	@MATRIX_RT_HOME=$(HOME_DIR)/run/compat-check setsid nohup $(HOME_DIR)/$(BIN) run --json > $(HOME_DIR)/run/compat-check/daemon.out 2>&1 < /dev/null & \
	sleep 1; \
	export MATRIX_RT_HOME=$(HOME_DIR)/run/compat-check; \
	$(HOME_DIR)/$(BIN) invoke 'ancient.api@1' '{"in":41}' --json | grep -q '"out":42' && echo "ancient live OK"; \
	$(HOME_DIR)/$(BIN) quit --json >/dev/null; sleep 0.5; \
	rm -rf $(HOME_DIR)/run/compat-check; \
	echo "compat PASS"

clean:
	rm -rf run/compat-check run/test-e2e
