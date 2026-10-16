DATA      ?=
OUT       ?=
EPOCHS    ?= 1
MAX_STEPS ?= 0
EXTRA     ?=

EXAMPLE_DATA := example/traces.jsonl
EXAMPLE_OUT  := example/runs/demo

.PHONY: install test check train example example-check example-train example-smoke clean-example

# Create the environment and install the SDK with its test dependencies.
install:
	uv sync --group dev

# Offline: no Kev, no GPU, no network.
test:
	uv run --group dev pytest

# Validate DATA and show the split, without training.
#   make check DATA=traces.jsonl
check:
	@test -n "$(DATA)" || { echo "usage: make check DATA=traces.jsonl"; exit 2; }
	uv run observer-finetune check --data $(DATA) $(EXTRA)

# Fine-tune on DATA and write the run to OUT.
#   make train DATA=traces.jsonl OUT=runs/my-run
#   make train DATA=phoenix://localhost:6006/observer-ai OUT=runs/my-run
#   EXTRA passes any other flag, e.g. EXTRA="--kev-url http://127.0.0.1:8009/v1/systemone"
train:
	@test -n "$(DATA)" -a -n "$(OUT)" || { echo "usage: make train DATA=traces.jsonl OUT=runs/my-run"; exit 2; }
	uv run observer-finetune train --data $(DATA) --out $(OUT) --epochs $(EPOCHS) --max-steps $(MAX_STEPS) $(EXTRA)

# Regenerate the synthetic example set (same file every time).
example:
	uv run python example/generate.py

example-check:
	uv run observer-finetune check --data $(EXAMPLE_DATA)

# Fine-tune on the example: about 15 minutes on an M3 Max. Fails if the run already exists;
# `make clean-example` removes it.
example-train:
	uv run observer-finetune train --data $(EXAMPLE_DATA) --out $(EXAMPLE_OUT)

# The same with three training steps, to check everything is wired up.
example-smoke:
	rm -rf $(EXAMPLE_OUT)-smoke
	uv run observer-finetune train --data $(EXAMPLE_DATA) --out $(EXAMPLE_OUT)-smoke --max-steps 3

# Remove the example's run directories (checkpoints and logs).
clean-example:
	rm -rf example/runs
