// Package common holds the types, interfaces and helpers that several of
// the engine's packages share and that are not part of the pipeline's
// domain (that is pkg/pipeline): the NATS connection and its pull loop, a
// worker pool, retry policies, and small concurrency helpers.
//
// It is internal: a public package may use it inside, but never exposes
// its types in a signature an embedding program has to fill in.
package common
