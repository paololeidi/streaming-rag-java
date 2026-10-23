# Reliability and Correctness Remediation Plan

This plan addresses the weaknesses identified during the project evaluation.
It complements the broader [engineering roadmap](improvements.md): prioritize
correctness and evidence before adding capabilities or optimizing throughput.
This is a backlog, not a statement that the changes are already implemented.

## Evidence and scope

The findings below come from inspecting the implementation and tests.
Failure scenarios described as acceptance criteria still need to be exercised.
The earlier Maven test run used Java 25 and encountered Byte Buddy compatibility
errors; it does not establish whether the suite passes on the intended Java 21
runtime. The full application and live LLM behavior have not been verified as
part of this review.

## P0: Make ingestion guarantees real

### 1. Make repeated ingestion idempotent

**Observed:** `VectorStoreRepository.store()` calls the store overload that
generates a random document ID. `LuceneEmbeddingStore.add()` appends a document,
even when supplied an existing ID. Redelivery can therefore duplicate evidence.

- [ ] Define event identity using the existing `SystemLog.eventId`, including
      the policy for a repeated ID with different content.
- [ ] Pass the stable ID through the repository and implement an idempotent
      Lucene write, such as `updateDocument()` keyed by `FIELD_ID`.
- [ ] Keep operational processing-attempt counters distinct from counts of
      unique indexed events.
- [ ] Test repeated writes, concurrent duplicate delivery, replay, and failure
      after indexing but before Kafka acknowledgment.

**Acceptance:** Reprocessing the same event leaves exactly one searchable
document under the documented identity policy. A retry does not inflate
statistics that claim to count unique events.

### 2. Align Kafka acknowledgment with durable Lucene storage

**Observed:** Writes refresh the searcher but do not explicitly commit the
Lucene index before acknowledging Kafka. Search visibility is not the same as
crash durability; normal writer shutdown is not a crash-recovery guarantee.

- [ ] Establish a durable-write boundary before committing the corresponding
      Kafka offsets. Start with the simplest correct commit strategy.
- [ ] Propagate indexing and commit errors so failed writes are not acknowledged.
- [ ] Preserve the ordering of durability and offset commits if batching is
      introduced later.
- [ ] Add an abrupt-process-termination recovery test using a disk-backed index.

**Acceptance:** Every Kafka-acknowledged event survives an abrupt application
restart. An event persisted before a failed acknowledgment can be redelivered
without duplication. Document this as at-least-once delivery with idempotent
indexing, not a distributed exactly-once transaction.

## P1: Honor API and tool contracts

### 3. Give the request's `k` parameter an enforceable meaning

**Observed:** `QueryController` logs `getEffectiveK()` but passes only the prompt
to the agent. The HTTP request value does not control tool retrieval.

- [ ] Define whether `k` limits each search or the complete response's sources,
      including defaults and a maximum.
- [ ] Enforce that contract in the retrieval execution path for synchronous
      and streaming requests; prompt instructions alone are not enforcement.
- [ ] Validate limits and return explicit validation errors for invalid inputs.
- [ ] Apply consistent bounds to MCP tool calls.

**Acceptance:** Deterministic tests show that different valid `k` values affect
the actual retrieval limit. Defaults, invalid values, upper bounds, and
concurrent requests are covered.

### 4. Implement truthful time-windowed statistics

**Observed:** `SreTools.logStats()` prints the requested window but reads
process-lifetime counters. Service names are hard-coded, and severity and
service counts are not computed over a jointly filtered event set.

- [ ] Index timestamps and filterable metadata as queryable Lucene fields,
      rather than keeping them only in stored metadata JSON.
- [ ] Compute totals and breakdowns from the same event set, applying the
      time, severity, and service filters together.
- [ ] Discover services from indexed data instead of a fixed list.
- [ ] Define UTC cutoff boundaries, late/future event behavior, and the
      distinction between event time and ingestion time.
- [ ] Use an injectable clock for deterministic tests.

**Acceptance:** Tests cover cutoff boundaries, combined filters, unknown
services, restart persistence, duplicate delivery, and arbitrary service names.
Until implemented, describe existing counters as "since startup" and do not
present them as windowed incident statistics.

### 5. Preserve request-scoped evidence across tool calls

**Observed:** A successful search replaces the captured source list. An empty
search leaves earlier sources unchanged. Capture uses `ThreadLocal`, while
streaming has asynchronous callbacks and no equivalent cleanup lifecycle.

- [ ] Define sources as retrieved evidence or verified answer citations; do not
      imply that every retrieved source was cited by the answer.
- [ ] Accumulate and deduplicate evidence across searches in one investigation.
- [ ] Use an explicit per-investigation context that remains correct across
      asynchronous execution, rather than assuming thread affinity.
- [ ] Define evidence bounds consistent with the `k` contract.
- [ ] Clean up context on success, failure, timeout, and cancellation.
- [ ] Report missing or malformed metadata explicitly rather than inventing
      event IDs or timestamps.

**Acceptance:** Two searches retain the appropriate evidence from both; an
empty search does not create misleading sources. Simultaneous requests remain
isolated, and streaming completion or failure leaves no stale context.

### 6. Distinguish actual token streaming from buffered delivery

**Observed:** The fallback waits for a complete answer, splits it into words,
then sends them with artificial delays. This is not token-by-token inference.
Disconnect handling does not explicitly cancel outstanding model work.

- [ ] Use genuine model streaming where supported, or explicitly document
      buffered delivery without artificial word delays.
- [ ] Restrict fallback to a recognized unsupported capability rather than
      treating any `IllegalArgumentException` as a streaming limitation.
- [ ] Define the terminal event, error behavior, and evidence payload.
- [ ] Handle timeout and disconnect lifecycle events and cancel outstanding
      work where the model client supports cancellation.

**Acceptance:** Tests consume actual SSE events and verify ordering, completion,
errors, timeout, and disconnect handling. Documentation accurately describes
time-to-first-output for every supported mode.

## P1: Make verification match the claims

### 7. Exercise the real pipeline in integration tests

**Observed:** `LogPipelineIntegrationTest` mocks `VectorStoreRepository`; it
cannot verify ONNX embedding or Lucene persistence. Its test profile disables
listener startup, while its Kafka test expects automatic consumption. Controller
tests invoke methods directly rather than testing MVC request handling.

- [ ] Separate mocked wiring tests from real pipeline integration tests.
- [ ] Enable Kafka listener startup explicitly for the real ingestion test.
- [ ] Use Testcontainers Kafka, the actual ONNX embedder, and an isolated Lucene
      directory. Verify retrieval of the exact produced event ID.
- [ ] Test disk persistence, duplicate delivery, write failure, retry, and DLQ
      behavior. Verify failed writes are not acknowledged.
- [ ] Manage containers through the test lifecycle; surface setup failures
      instead of swallowing topic-creation exceptions.
- [ ] Add MVC tests for validation, JSON contracts, and asynchronous SSE output.
- [ ] Require the real integration suite in Docker-enabled CI. A documented
      local skip is acceptable; a required CI suite must not silently skip.

**Acceptance:** CI proves Kafka-to-embedding-to-index retrieval without mocking
the components under test. It fails when listeners do not run, persistence
fails, or required infrastructure is unavailable.

### 8. Establish a supported runtime baseline

- [ ] Re-run the suite on Java 21 before attributing Java 25 failures to
      application defects.
- [ ] State the tested JDK range precisely; avoid implying all newer JDKs are
      supported without evidence.
- [ ] If newer JDK support is intended, update compatible test dependencies
      and add those JDKs to CI rather than relying on experimental JVM flags.

**Acceptance:** The documented build command passes on the supported JDK, and CI
tests every runtime claimed as supported.

## P2: Establish useful quality and operational evidence

### 9. Measure retrieval and answer quality

- [ ] Create deterministic incident fixtures with expected relevant event IDs,
      including exact error codes, trace IDs, and misleading similar messages.
- [ ] Measure retrieval recall at fixed `k` values before changing the model.
- [ ] Add lexical/hybrid retrieval and metadata filtering where benchmarks show
      dense retrieval misses essential evidence.
- [ ] Evaluate answer grounding, evidence coverage, and behavior when the
      available logs cannot establish a root cause. Separate deterministic
      retrieval tests from model-dependent answer evaluations.

**Acceptance:** Publish a reproducible baseline and explicit regression
thresholds. Assess improvements against the same fixtures, not just demo answers.

### 10. Bound resource usage and document deployment limits

- [ ] Implement and test a configurable retention policy for the index.
- [ ] Add dependency-aware readiness and separate it from process liveness.
- [ ] Expose ingestion lag, indexing failures, index growth, retrieval latency,
      and model/tool latency.
- [ ] Document a supported single-instance topology before advertising
      horizontal scaling: Kafka consumer replicas with independent embedded
      indexes each see only part of the data.
- [ ] Benchmark sustained ingestion and concurrent queries on specified
      hardware before claiming throughput or concurrency limits.

**Acceptance:** Expired records are removed as documented, dependency failures
affect readiness appropriately, and deployment instructions explain index
ownership, persistence, recovery, and scaling limitations.

### 11. Make documentation evidence-based

- [ ] Describe the current system as a prototype and distinguish implemented,
      tested, benchmarked, and planned capabilities.
- [ ] Correct guarantees about zero log loss, chronological ordering,
      scalability, and streaming to match observed behavior.
- [ ] Remove unsupported performance comparisons and explain that virtual
      threads improve blocking-I/O concurrency, not CPU-bound embedding capacity.
- [ ] Correct the roadmap's claims of full end-to-end and MVC coverage until
      those tests exist. Citation capture currently uses metadata and a
      thread-local list, not parsing tool-output strings.
- [ ] Replace machine-local documentation links with repository-relative links.

**Acceptance:** Each advertised guarantee points to an implementation and
appropriate test or benchmark. Known limits are visible in the quick start.

## Suggested implementation order

1. Establish the Java 21 baseline and repair the integration-test harness.
2. Implement durable, idempotent ingestion and recovery tests.
3. Fix retrieval limits, statistics, and evidence handling.
4. Correct streaming semantics and lifecycle handling.
5. Add quality evaluation, retention, observability, and measured benchmarks.

Update documentation alongside each behavioral change rather than waiting for
the whole plan to be complete. Coordinate overlapping work with roadmap items
J0.2, 1.3, 1.4, 2.2, 3.1-3.3, 4.3, and 4.4.
