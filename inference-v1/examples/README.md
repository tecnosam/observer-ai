# Engine config examples

Ready-to-run variations of the engine's config file. Each one sets only what
differs from the defaults in [`../engine/observer.yaml`](../engine/observer.yaml);
every key it leaves out keeps its default.
[`../engine/observer.schema.yaml`](../engine/observer.schema.yaml) documents
every key, and a test (`internal/config/examples_test.go`) checks that each
example here loads.

| File | Use it to |
|---|---|
| [`jev.yaml`](jev.yaml) | Score with TypeSafe's hosted Jev instead of a Kev server |
| [`kev-sdk-run.yaml`](kev-sdk-run.yaml) | Serve a Kev run made by the fine-tuning SDK |
| [`json-api-only.yaml`](json-api-only.yaml) | Take traces over the JSON API only, with the OTLP receivers off |
| [`review-more.yaml`](review-more.yaml) | Keep more traces for review: a lower threshold and a larger random sample |
| [`remote-services.yaml`](remote-services.yaml) | Run against Kev, NATS and a hosted Phoenix on other machines |
| [`langfuse.yaml`](langfuse.yaml) | Send kept traces to Langfuse as well as Phoenix |

## Running one

From `inference-v1/engine`, with Phoenix up (`make phoenix`). The queues
stay in memory unless the config or `OBSERVER_NATS_URL` sets `queue.url`
(then start NATS with `make nats`):

```sh
make run CONFIG=../examples/review-more.yaml
```

A relative path inside a config (such as `scorer.manifest`) resolves against
the config file's own directory, so the examples work from wherever you start
the engine.

## Jev

```sh
export JEV_API_KEY=...          # or keep it in envchain: envchain observer-ai make run CONFIG=../examples/jev.yaml
make run CONFIG=../examples/jev.yaml
```

`scorer.kind: jev` makes the engine send the key as a bearer token with every
request. It reads the key from the environment variable named in
`scorer.api_key_env`, `JEV_API_KEY` unless you change it, and refuses to start
if that variable is empty. The key itself never goes in the file.

Jev is not fine-tuned, so it needs no run directory. Its manifest,
[`manifests/jev.json`](manifests/jev.json), holds the questions the engine asks
(the same ones Kev is trained on) and a temperature per question. They are
1.0, Jev's raw probabilities: fit them on calibration traces from your own
agent before relying on the thresholds.

Two things to know before pointing it at real traffic:

- **Every trace slice leaves your network.** It is sent to `api.typesafe.ai`
  and billed to the key. Only use it for traces you are allowed to send to a
  third party.
- **Rate limits.** Jev answers 429 when busy. The engine does not retry a 429
  itself; the trace goes back to the queue and is redelivered later with
  `queue.redelivery_backoff`. If that happens often, lower `server.workers`.

## Sending traces to Langfuse, or any OTLP back end

`exporters` is a list: every kept trace goes to each back end in it, each
with its own queue, so a slow one never holds up the others.
[`langfuse.yaml`](langfuse.yaml) keeps the local Phoenix and adds Langfuse,
which takes Basic auth built from your public and secret keys. Keep the
header in an environment variable and name it in `headers_env`:

```sh
export LANGFUSE_OTLP_HEADERS="Authorization=Basic%20$(printf '%s' "pk-lf-...:sk-lf-..." | base64)"
make run CONFIG=../examples/langfuse.yaml
```

The variable holds headers in the OpenTelemetry format, `key=value` pairs
separated by commas, with `%20` for a space. The engine refuses to start if
it is unset. Any other back end that takes OTLP/HTTP traces (Honeycomb, Grafana
Tempo, an OpenTelemetry Collector) is one more entry: a `name`, its traces
`endpoint` with the path, and its auth header. The engine's README lists the
attributes each back end receives.

## Docker Compose

The compose file bakes `engine/observer.yaml` into the image and sets the
scorer and NATS URLs and the first exporter's endpoint from environment
variables (`OBSERVER_EXPORT_ENDPOINT` for the exporter), which override the
same keys in any config. To run the Jev example in Docker, mount it over the
baked-in config and pass the key through, in `engine/docker-compose.yml`:

```yaml
  observer:
    environment:
      JEV_API_KEY: ${JEV_API_KEY}
    volumes:
      - ../examples/jev.yaml:/etc/observer/observer.yaml:ro
```

and start it with Jev's endpoint and manifest, since the compose defaults point
at a local Kev:

```sh
OBSERVER_SCORER_ENDPOINT=https://api.typesafe.ai/v1/systemone \
OBSERVER_MANIFEST_FILE=../examples/manifests/jev.json \
JEV_API_KEY=... docker compose up --build -d
```
