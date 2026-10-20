BIN    := bin/observer
CONFIG ?= observer.yaml
PYTHON ?= python3
FUZZ   := $(or $(TMPDIR),/tmp)/observer-fuzz-golden.jsonl

NATS_BOX := docker run --rm --network observer_default natsio/nats-box:latest nats -s nats://nats:4222

.PHONY: chart-lint build test run lint fmt golden fuzz-golden clean nats phoenix up down logs dlq agent

build:
	go build -o $(BIN) ./cmd/observer

# Offline: tests use httptest servers, in-memory span recorders and an
# in-process NATS server on a loopback port.
test:
	go vet ./...
	go test -race ./...

# Queues in memory by default. For NATS: `make nats` and
# OBSERVER_NATS_URL=nats://localhost:4222 make run. Needs Phoenix on
# localhost:6006 (`make phoenix`) and Kev.
run: build
	$(BIN) -config $(CONFIG)

# Start only NATS JetStream, for `OBSERVER_NATS_URL=nats://localhost:4222 make run`.
nats:
	docker compose up -d nats

# Start only Phoenix, for running the engine on the host with `make run`.
phoenix:
	docker compose up -d phoenix

# Build and start the engine, NATS and Phoenix in Docker.
up:
	docker compose up --build -d

# Stop all three. Queued traces and dead letters stay in the nats-data
# volume, Phoenix's traces in phoenix-data.
down:
	docker compose down

logs:
	docker compose logs -f observer

# Start the test agent's chat UI in the browser, exporting its traces to this
# engine. Needs Ollama. See finetune/testagents.
agent:
	$(MAKE) -C ../../finetune/testagents chat EXPORTING=1

# Show the dead-letter stream. Needs the compose stack to be up.
dlq:
	$(NATS_BOX) stream info OBSERVER_TRACES_DLQ

lint:
	@unformatted="$$(gofmt -l .)"; if [ -n "$$unformatted" ]; then \
		echo "gofmt needed on:"; echo "$$unformatted"; exit 1; fi
	go vet ./...

fmt:
	gofmt -w .

# Regenerate testdata/golden/*.txt from the Python training code.
golden:
	$(PYTHON) testdata/gen_golden.py

# Compare BuildState with Python over a few thousand randomized traces,
# including every Unicode code point.
fuzz-golden:
	$(PYTHON) testdata/gen_fuzz.py $(FUZZ)
	OBSERVER_FUZZ_GOLDEN=$(FUZZ) go test ./pkg/trace -run TestBuildStateFuzzGolden -count=1 -v
	rm -f $(FUZZ)

clean:
	rm -rf bin

# Lint the Helm chart and render its main setups (needs helm).
CHART := deploy/helm/observer
CHART_MANIFEST := --set-file 'manifests.files.manifest\.json=testdata/manifest.json'
chart-lint:
	helm dependency update $(CHART)
	helm lint $(CHART) $(CHART_MANIFEST)
	helm template observer $(CHART) $(CHART_MANIFEST) > /dev/null
	helm template observer $(CHART) $(CHART_MANIFEST) --set nats.enabled=true > /dev/null
	helm template observer $(CHART) $(CHART_MANIFEST) --set traceRouting.gateway.enabled=true > /dev/null
	helm template observer $(CHART) $(CHART_MANIFEST) --set nats.enabled=true --set autoscaling.queueDepth.enabled=true > /dev/null
