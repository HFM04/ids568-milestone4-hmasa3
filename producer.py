"""
producer.py
-----------
This script simulates a real-time event stream of transaction events.

WHAT IS A PRODUCER?
In streaming systems, a "producer" is the component that generates events
and puts them into a queue. Think of it like a ticket printer at a deli:
- The producer prints tickets (events) at some rate
- The consumer processes them in order

WHY STREAMING?
Batch processing (like our pipeline.py) works well for historical data.
But what if you need to detect fraud in REAL TIME — while the transaction
is happening? That requires streaming.

HOW THIS WORKS:
We simulate a "queue" using a shared in-memory list (or a file).
In production, this would be Apache Kafka or Amazon Kinesis.
We use a simple approach here so no external software is needed.

Usage:
    # Terminal 1: Start producer
    python producer.py --rate 100 --duration 30 --output events.jsonl

    # Terminal 2: Start consumer (in another terminal)
    python consumer.py --input events.jsonl --window 5
"""

import argparse
import json
import os
import random
import time
import threading

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────
PRODUCT_CATEGORIES = ["Electronics", "Clothing", "Food", "Books", "Toys", "Sports"]
REGIONS = ["North", "South", "East", "West", "Central"]
PAYMENT_METHODS = ["Credit Card", "Debit Card", "PayPal", "Cash", "Gift Card"]

# These rates simulate different real-world traffic patterns
TRAFFIC_PATTERNS = {
    "steady":   {"base_rate": 1.0,  "burst_multiplier": 1.0},  # Constant flow
    "bursty":   {"base_rate": 0.5,  "burst_multiplier": 5.0},  # Quiet then spike
    "growing":  {"base_rate": 0.5,  "burst_multiplier": 3.0},  # Load test
}


def generate_event(event_id: int, timestamp: float) -> dict:
    """
    Generate a single transaction event with a timestamp.

    Unlike batch data, streaming events carry a precise timestamp so
    consumers know exactly WHEN things happened. This is critical for
    windowing (e.g., "count transactions in the last 5 seconds").

    Parameters:
        event_id: Unique sequential ID for this event
        timestamp: Unix timestamp (seconds since Jan 1, 1970)

    Returns:
        dict: Event data
    """
    amount = round(random.uniform(1.0, 999.99), 2)
    quantity = random.randint(1, 20)
    is_fraud = random.random() < 0.02  # 2% fraud rate

    return {
        "event_id": event_id,
        "timestamp": timestamp,
        "user_id": random.randint(1, 100_000),
        "product_category": random.choice(PRODUCT_CATEGORIES),
        "region": random.choice(REGIONS),
        "amount": amount,
        "quantity": quantity,
        "total": round(amount * quantity, 2),
        "payment_method": random.choice(PAYMENT_METHODS),
        "is_fraud": is_fraud,
    }


def produce_events(
    output_file: str,
    target_rate: int,
    duration_seconds: int,
    pattern: str,
    seed: int
):
    """
    Generate a stream of events and write them to a file, one per line.

    We use JSONL format (JSON Lines): each line is a complete JSON object.
    This lets the consumer read events one line at a time as they arrive.

    Parameters:
        output_file: Where to write events (appends line by line)
        target_rate: How many events per second to target
        duration_seconds: How long to run (0 = run forever)
        pattern: Traffic pattern ("steady", "bursty", or "growing")
        seed: Random seed for reproducibility
    """
    random.seed(seed)

    pat = TRAFFIC_PATTERNS.get(pattern, TRAFFIC_PATTERNS["steady"])

    print(f"\n{'='*60}")
    print(f"PRODUCER STARTING")
    print(f"{'='*60}")
    print(f"  Output file:  {output_file}")
    print(f"  Target rate:  {target_rate} events/second")
    print(f"  Duration:     {duration_seconds}s (0 = indefinite)")
    print(f"  Pattern:      {pattern}")
    print(f"  Seed:         {seed}")
    print(f"{'='*60}\n")

    # Ensure output directory exists
    os.makedirs(os.path.dirname(output_file) if os.path.dirname(output_file) else ".", exist_ok=True)

    event_id = 0
    start_time = time.time()
    last_report_time = start_time
    events_since_report = 0
    total_events = 0

    # Track production metrics
    metrics = {
        "events_produced": 0,
        "start_time": start_time,
        "rates_per_second": [],
        "pattern": pattern,
    }

    # Open in append mode ("a") so consumer can read while we write
    with open(output_file, "a") as f:

        while True:
            loop_start = time.time()
            elapsed = loop_start - start_time

            # Stop if duration exceeded (unless duration=0 = run forever)
            if duration_seconds > 0 and elapsed >= duration_seconds:
                break

            # ── SIMULATE TRAFFIC PATTERNS ─────────────────────────────────────

            if pattern == "bursty":
                # Every 10 seconds, create a burst of traffic
                # This simulates flash sales or peak hours
                cycle_pos = elapsed % 10
                if cycle_pos < 2:  # Burst for 2 seconds
                    current_rate = int(target_rate * pat["burst_multiplier"])
                else:              # Quiet for 8 seconds
                    current_rate = int(target_rate * pat["base_rate"])

            elif pattern == "growing":
                # Gradually increase load — simulates a load test
                # Rate grows from base to max over the duration
                if duration_seconds > 0:
                    progress = elapsed / duration_seconds
                else:
                    progress = min(elapsed / 60, 1.0)  # Cap at 60s growth
                current_rate = int(
                    target_rate * pat["base_rate"] +
                    target_rate * (pat["burst_multiplier"] - pat["base_rate"]) * progress
                )

            else:  # steady
                current_rate = target_rate

            # ── GENERATE EVENTS FOR THIS SECOND ──────────────────────────────

            # How many events to generate in this 1-second tick
            # Add small randomness to simulate real-world variability
            events_this_second = max(1, int(current_rate * random.uniform(0.8, 1.2)))

            batch_events = []
            for _ in range(events_this_second):
                event_time = time.time()
                event = generate_event(event_id, event_time)
                batch_events.append(json.dumps(event))
                event_id += 1

            # Write all events for this second at once (more efficient)
            f.write("\n".join(batch_events) + "\n")
            f.flush()  # Flush to disk so consumer can read immediately

            total_events += events_this_second
            events_since_report += events_this_second

            # ── PRINT PROGRESS REPORT EVERY 5 SECONDS ─────────────────────────

            now = time.time()
            if now - last_report_time >= 5:
                actual_rate = events_since_report / (now - last_report_time)
                metrics["rates_per_second"].append(actual_rate)

                print(
                    f"[{elapsed:6.1f}s] "
                    f"Events produced: {total_events:>8,} | "
                    f"Rate: {actual_rate:>7.1f} ev/s | "
                    f"Pattern: {pattern} ({current_rate} target)"
                )
                last_report_time = now
                events_since_report = 0

            # ── RATE LIMITING ─────────────────────────────────────────────────

            # How long did generating events take?
            loop_elapsed = time.time() - loop_start

            # Sleep for the remainder of 1 second
            # This ensures we don't exceed our target rate
            sleep_time = max(0, 1.0 - loop_elapsed)
            if sleep_time > 0:
                time.sleep(sleep_time)

    # Final summary
    total_time = time.time() - start_time
    metrics["events_produced"] = total_events
    metrics["total_time"] = total_time
    metrics["avg_rate"] = total_events / total_time if total_time > 0 else 0

    print(f"\n{'='*60}")
    print(f"PRODUCER FINISHED")
    print(f"  Total events:  {total_events:,}")
    print(f"  Total time:    {total_time:.1f}s")
    print(f"  Avg rate:      {metrics['avg_rate']:.1f} events/second")
    print(f"{'='*60}\n")

    # Save producer metrics
    metrics_path = output_file.replace(".jsonl", "_producer_metrics.json")
    with open(metrics_path, "w") as f:
        json.dump(metrics, f, indent=2)

    return metrics


def main():
    parser = argparse.ArgumentParser(
        description="Generate a stream of synthetic transaction events.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Traffic Patterns:
  steady  - Constant rate (good for baseline testing)
  bursty  - Alternates between quiet and burst periods (realistic)
  growing - Gradually increases load (good for finding breaking point)

Examples:
  # Steady stream at 100 events/second for 60 seconds
  python producer.py --rate 100 --duration 60 --pattern steady

  # Bursty traffic to test backpressure handling
  python producer.py --rate 500 --duration 120 --pattern bursty

  # Load test: gradually increase to find breaking point
  python producer.py --rate 2000 --duration 180 --pattern growing
        """
    )

    parser.add_argument("--rate", type=int, default=100,
                        help="Target events per second (default: 100)")
    parser.add_argument("--duration", type=int, default=60,
                        help="Duration in seconds, 0 = run forever (default: 60)")
    parser.add_argument("--output", type=str, default="stream/events.jsonl",
                        help="Output file path (default: stream/events.jsonl)")
    parser.add_argument("--pattern", type=str, default="steady",
                        choices=["steady", "bursty", "growing"],
                        help="Traffic pattern (default: steady)")
    parser.add_argument("--seed", type=int, default=42,
                        help="Random seed (default: 42)")

    args = parser.parse_args()

    produce_events(
        output_file=args.output,
        target_rate=args.rate,
        duration_seconds=args.duration,
        pattern=args.pattern,
        seed=args.seed,
    )


if __name__ == "__main__":
    main()
