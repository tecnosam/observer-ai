BIN    := bin/observer
CONFIG ?= observer.yaml
PYTHON ?= python3
FUZZ   := $(or $(TMPDIR),/tmp)/observer-fuzz-golden.jsonl

.PHONY: build test run lint fmt golden fuzz-golden clean

build:
	go build -o $(BIN) ./cmd/observer

# Offline: every test uses httptest servers and in-memory span recorders.
test:
	go vet ./...
	go test -race ./...

run: build
	$(BIN) -config $(CONFIG)

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
