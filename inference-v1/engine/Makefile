BIN    := bin/observer
CONFIG ?= observer.yaml
PYTHON ?= python3
FUZZ   := $(or $(TMPDIR),/tmp)/observer-fuzz-golden.jsonl

NATS_BOX := docker run --rm --network observer_default natsio/nats-box:latest nats -s nats://nats:4222

.PHONY: build test run lint fmt golden fuzz-golden clean nats up down logs dlq

build:
	go build -o $(BIN) ./cmd/observer

# Offline: tests use httptest servers, in-memory span recorders and an
# in-process NATS server on a loopback port.
test:
	go vet ./...
	go test -race ./...

# Needs NATS on localhost:4222 (`make nats`), plus Kev and Phoenix.
run: build
	$(BIN) -config $(CONFIG)

# Start only NATS JetStream, for running the engine on the host with `make run`.
nats:
	docker compose up -d nats

# Build and start the engine and NATS in Docker.
up:
	docker compose up --build -d

# Stop both. Queued traces and dead letters stay in the nats-data volume.
down:
	docker compose down

logs:
	docker compose logs -f observer

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
	OBSERVER_FUZZ_GOLDEN=$(FUZZ) go test ./internal/trace -run TestBuildStateFuzzGolden -count=1 -v
	rm -f $(FUZZ)

clean:
	rm -rf bin
