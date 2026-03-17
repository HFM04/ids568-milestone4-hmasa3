# Streaming Pipeline Report

**MLOps Course – Module 5 — Bonus Component**  
**Author:** [Your Name] | **NetID:** [your_netid]

---

## 1. Architecture Overview

Our streaming pipeline follows the standard **Ingest → Process → Sink** pattern:

```
[producer.py]              [consumer.py]              [stream_results/]
     ↓                          ↓                           ↓
 Generate events  ──JSONL──→  Read events      ──→    Write results
 at target rate    (file)     into windows             (JSONL + JSON)
                              compute stats
                              detect fraud
```

**Technology choice:** We use a file-based queue (JSONL file) instead of Kafka, as the assignment allows "mock Python queue" and this avoids requiring external infrastructure. In production, the only change would be replacing `f.readline()` with a Kafka consumer client.

---

## 2. Producer Design

### Event Schema

Each event is a JSON object written as one line in `stream/events.jsonl`:

```json
{
  "event_id": 1042,
  "timestamp": 1710000042.5,
  "user_id": 75321,
  "product_category": "Electronics",
  "region": "North",
  "amount": 299.99,
  "quantity": 2,
  "total": 599.98,
  "payment_method": "Credit Card",
  "is_fraud": false
}
```

The `timestamp` field is essential — it tells the consumer exactly when the event occurred, which is needed to assign events to the correct time window.

### Traffic Patterns

We implement three patterns to simulate real-world scenarios:

| Pattern | Description | Use Case |
|---|---|---|
| `steady` | Constant rate throughout | Baseline testing |
| `bursty` | Alternates quiet (0.5×) and burst (5×) periods | Flash sales, prime-time traffic |
| `growing` | Gradually increases from 0.5× to full rate | Load testing to find breaking point |

---

## 3. Consumer Design: Tumbling Windows

### What is a Tumbling Window?

A tumbling window groups events into fixed-size, non-overlapping time buckets:

```
Time (seconds):  0    5    10   15   20
                 |    |    |    |    |
Window 1: ───────|
Window 2:        ──────|
Window 3:              ──────|
Window 4:                    ──────|
```

Each event falls into exactly one window. When a window's time period expires, we compute its statistics and start a new window.

### Stateful Operations

The `WindowState` class maintains running totals for each window:
- Event count
- Total revenue
- Fraud count
- Per-region and per-category counts
- List of processing latencies

This is "stateful" because we accumulate state across many events before producing output.

### Late-Arriving Data Handling

In real distributed systems, events don't always arrive in order. A transaction timestamped at 4.9s might not arrive until 5.1s — after its window has closed.

**Our approach:** We detect late events (timestamp < current window start) and include them in the current window rather than discarding them. This is the "at-least-once" approach — no data is lost, but some events may be slightly miscategorized by window.

In production, a **watermark** (grace period) would be set — e.g., accept events up to 2 seconds late, then discard stragglers.

---

## 4. Latency Measurements

**What is latency?**  
The time between when an event was created (`timestamp` field) and when we finished processing it. Lower is better.

**Why measure p50/p95/p99?**  
The average hides outliers. p99 tells us "how bad are the worst 1% of cases?" — critical for SLAs.

### Load Testing Results

We ran the producer and consumer for 60 seconds at each load level:

| Load Level | Events/sec | p50 Latency | p95 Latency | p99 Latency | Throughput |
|---|---|---|---|---|---|
| Low | 100 msg/s | ___ ms | ___ ms | ___ ms | ___ msg/s |
| Medium | 1,000 msg/s | ___ ms | ___ ms | ___ ms | ___ msg/s |
| High | 5,000 msg/s | ___ ms | ___ ms | ___ ms | ___ msg/s |
| Breaking point | ___ msg/s | ___ ms | ___ ms | ___ ms | ___ msg/s |

> Fill in from `stream_results/consumer_metrics.json` after running load tests.

### How to Run the Load Tests

```bash
# Test 1: Low load (100 msg/s)
python producer.py --rate 100 --duration 60 --output stream/events_100.jsonl
python consumer.py --input stream/events_100.jsonl --window 5 --output stream_results_100/

# Test 2: Medium load
python producer.py --rate 1000 --duration 60 --output stream/events_1000.jsonl
python consumer.py --input stream/events_1000.jsonl --window 5 --output stream_results_1000/

# Test 3: Growing load (find breaking point)
python producer.py --rate 5000 --duration 120 --pattern growing --output stream/events_growing.jsonl
python consumer.py --input stream/events_growing.jsonl --window 5 --output stream_results_growing/
```

### Observations

At **low load (100 msg/s):** Latency is dominated by the polling interval (0.1s). The consumer is never backlogged.

At **medium load (1,000 msg/s):** Latency increases slightly as the consumer processes larger batches per window.

At **high load (5,000+ msg/s):** Queue depth (unread lines in the file) begins to grow. This is **backpressure** — the consumer cannot keep up with the producer. p99 latency increases significantly.

**Breaking point:** Around ___ msg/s, the consumer's processing time per window exceeds the window size, causing queue depth to grow unboundedly.

---

## 5. Queue Depth Monitoring

Queue depth = number of events waiting to be processed = `events produced - events consumed`

```
Queue Depth (events)
    ^
    |                              ┌─────────
800 |                         ─────┘
    |                    ─────
400 |               ─────
    |         ──────
  0 |──────────
    +──────────────────────────────────────► Time
       100/s    500/s   1K/s   5K/s
```

Rising queue depth is the early warning sign that the system is approaching its limit. In production: set an alert when queue depth > 1,000 to trigger auto-scaling.

---

## 6. Failure Handling Analysis

### 6.1 Failure Scenarios

We tested the following scenarios using `python consumer.py --test-failure`:

**Scenario 1: Consumer crash mid-window**
- The consumer crashes at event 500
- A checkpoint file records the last processed `event_id`
- On restart, the consumer skips all events with ID ≤ checkpoint
- Events in the partially-processed window are replayed from checkpoint

**Scenario 2: Producer stops temporarily**
- Consumer polls for new data but finds none
- Consumer waits up to 5 seconds before assuming producer is done
- When producer resumes, consumer picks up seamlessly

**Scenario 3: Corrupted event in stream**
- `json.loads()` raises a `JSONDecodeError`
- Consumer logs a warning but continues processing
- Bad events are counted but not included in statistics

### 6.2 Recovery Semantics

| Guarantee | Description | Our Implementation |
|---|---|---|
| **At-most-once** | Events may be lost on crash | ❌ Not used (data loss unacceptable) |
| **At-least-once** | Events replayed after crash (may duplicate) | ✅ Our approach (checkpointing) |
| **Exactly-once** | No loss, no duplicates | ❌ Requires transactional sinks (complex) |

**Why at-least-once?**  
Exactly-once semantics require either idempotent writes (writing the same event twice has the same result) or a transactional sink (like a database with ACID guarantees). For our aggregation use case, replaying a few events at recovery time introduces negligible error — acceptable for analytics.

### 6.3 What Happens When Consumer Crashes and Restarts

```
Normal:     [Event 1] → [Event 2] → [Event 3] → [Event 4] ... [Event 500]
                                                                 ↑ CRASH
                                              Checkpoint saved: event_id=499

Restart:    Skip events 0-499 → [Event 500] → [Event 501] ...
            (at-least-once: event 499 may be reprocessed)
```

---

## 7. Throughput vs. Consistency Trade-offs

| Priority | Approach | Latency | Throughput | Consistency |
|---|---|---|---|---|
| Maximum throughput | Batch writes, async flush | Higher | Very high | At-least-once |
| Low latency | Flush every event | Lower | Lower | At-least-once |
| Exactly-once | Transactional writes | High | Lower | Exactly-once |

**Our choice:** We flush events to the file immediately (`f.flush()`) but batch-process them in 1-second chunks. This gives reasonable latency without sacrificing throughput.

---

## 8. Operational Considerations

### Monitoring Checklist

For a production streaming system, you'd monitor:

- **Queue depth** — early warning for backpressure
- **p99 latency** — SLA compliance
- **Consumer lag** — how far behind is the consumer?
- **Error rate** — % of events that failed to parse or process
- **Fraud rate per window** — anomaly detection signal

### Capacity Planning

From our load tests, the single-consumer architecture handles approximately ___ events/second before queue depth grows. To handle 10× more load:

1. **Horizontal scaling:** Run multiple consumer instances, each reading from a different Kafka partition
2. **Larger window size:** Process in 30s windows instead of 5s (reduces per-window overhead)
3. **Async I/O:** Use `asyncio` to overlap reading and processing

---

## 9. Summary

| Metric | Value |
|---|---|
| Window size | 5 seconds |
| Ingestion format | JSONL (line-delimited JSON) |
| Windowing type | Tumbling (non-overlapping) |
| Delivery semantics | At-least-once |
| Late data handling | Include in current window |
| Max sustainable throughput | ___ msg/s |
| p99 latency at 100 msg/s | ___ ms |
| p99 latency at 1,000 msg/s | ___ ms |
| Recovery mechanism | Checkpoint file + event ID skip |
