"""
consumer.py
-----------
This script reads the event stream produced by producer.py and processes
events in real time using windowed aggregations.

WHAT IS A CONSUMER?
A consumer reads events from a queue and does something with them.
In our case, we:
1. Read events from the JSONL file (line by line, as they arrive)
2. Group events into time windows (e.g., "last 5 seconds")
3. Compute statistics for each window
4. Detect anomalies (e.g., fraud spikes)
5. Write results and measure latency

WHAT IS WINDOWING?
Instead of processing one event at a time, windowing groups events into
time-based buckets. For example, a 5-second tumbling window:

 Time:  0─────5─────10─────15─────20
        [Window 1] [Window 2] [Window 3]

Each window is processed independently when it "closes" (time expires).

WHAT IS LATENCY?
The time between when an event is created (timestamp in the event) and
when we finish processing it. Lower = better.
- p50 = median latency (50% of events processed faster than this)
- p95 = 95% of events processed faster than this (the "slow" ones)
- p99 = 99% processed faster (catches the very worst cases)

Usage:
    python consumer.py --input stream/events.jsonl --window 5 --duration 60
"""

import argparse
import collections
import json
import os
import statistics
import time
from typing import Dict, List, Optional

# ─────────────────────────────────────────────────────────────────────────────
# DATA STRUCTURES
# ─────────────────────────────────────────────────────────────────────────────

class WindowState:
    """
    Tracks the state of a single time window.

    A window is like a temporary accumulator that collects statistics
    about events that arrived in a specific time period.

    Attributes:
        window_id: Sequential ID for this window
        start_time: When this window started (Unix timestamp)
        end_time: When this window will close
        events: List of all events in this window
        latencies: Processing latency (seconds) for each event
    """

    def __init__(self, window_id: int, start_time: float, window_size: float):
        self.window_id = window_id
        self.start_time = start_time
        self.end_time = start_time + window_size
        self.window_size = window_size

        # Accumulate stats without storing all events (memory efficient)
        self.event_count = 0
        self.total_revenue = 0.0
        self.fraud_count = 0
        self.latencies: List[float] = []  # Processing latency per event
        self.region_counts: Dict[str, int] = collections.defaultdict(int)
        self.category_counts: Dict[str, int] = collections.defaultdict(int)

        # Track if this window got late-arriving data
        # (events with timestamps before the window closed)
        self.late_events = 0

    def add_event(self, event: dict, processing_latency: float):
        """
        Add an event's data to this window's accumulators.

        Parameters:
            event: The transaction event dict
            processing_latency: How long (seconds) since event was created
        """
        self.event_count += 1
        self.total_revenue += event.get("total", 0)
        if event.get("is_fraud", False):
            self.fraud_count += 1
        self.latencies.append(processing_latency)
        self.region_counts[event.get("region", "Unknown")] += 1
        self.category_counts[event.get("product_category", "Unknown")] += 1

    def compute_stats(self) -> dict:
        """Compute summary statistics for this completed window."""
        if not self.latencies:
            return {}

        sorted_lat = sorted(self.latencies)
        n = len(sorted_lat)

        def percentile(data, p):
            """Compute the p-th percentile of a sorted list."""
            idx = min(int(p / 100 * n), n - 1)
            return data[idx]

        return {
            "window_id": self.window_id,
            "window_start": self.start_time,
            "window_end": self.end_time,
            "duration_seconds": self.window_size,
            "event_count": self.event_count,
            "throughput_per_second": self.event_count / self.window_size,
            "total_revenue": round(self.total_revenue, 2),
            "fraud_count": self.fraud_count,
            "fraud_rate_pct": round(self.fraud_count / max(self.event_count, 1) * 100, 4),
            "latency_p50_ms": round(percentile(sorted_lat, 50) * 1000, 2),
            "latency_p95_ms": round(percentile(sorted_lat, 95) * 1000, 2),
            "latency_p99_ms": round(percentile(sorted_lat, 99) * 1000, 2),
            "latency_max_ms": round(max(sorted_lat) * 1000, 2),
            "late_events": self.late_events,
            "top_region": max(self.region_counts, key=self.region_counts.get) if self.region_counts else "N/A",
            "top_category": max(self.category_counts, key=self.category_counts.get) if self.category_counts else "N/A",
        }


class TumblingWindowProcessor:
    """
    Processes events using tumbling (non-overlapping) time windows.

    TUMBLING WINDOWS: Each event belongs to exactly ONE window.
    The window "tumbles" forward when it closes.

    Timeline example (5-second windows):
      [0-5s: Window 1] → process & report
      [5-10s: Window 2] → process & report
      [10-15s: Window 3] → process & report

    WHY TUMBLING WINDOWS?
    - Simple to understand and implement
    - Good for periodic reports ("every 5 seconds, how many transactions?")
    - No double-counting (each event in exactly one window)

    LATE ARRIVING DATA:
    Events may arrive "late" — their timestamp says 4.9s but we received
    them at 5.1s, after the window closed. We handle these by checking if
    the event's timestamp falls before the current window start.
    """

    def __init__(self, window_size: float, results_dir: str):
        """
        Initialize the processor.

        Parameters:
            window_size: Length of each window in seconds
            results_dir: Where to save window results
        """
        self.window_size = window_size
        self.results_dir = results_dir
        os.makedirs(results_dir, exist_ok=True)

        self.current_window: Optional[WindowState] = None
        self.completed_windows: List[dict] = []
        self.total_events_processed = 0
        self.all_latencies: List[float] = []

        # This file collects all window results
        self.results_file = os.path.join(results_dir, "window_results.jsonl")
        # Clear it at start
        open(self.results_file, "w").close()

    def get_window_for_time(self, event_time: float) -> int:
        """
        Determine which window ID a given timestamp belongs to.

        Example: window_size=5, event_time=12.3 → window_id=2
        (Window 2 covers seconds 10-15)
        """
        return int(event_time / self.window_size)

    def process_event(self, event: dict):
        """
        Route an event to the appropriate window and accumulate stats.

        Parameters:
            event: The transaction event dict
        """
        # Record when WE received/processed this event
        processing_time = time.time()

        # The event's timestamp tells us WHEN it was created
        event_time = event.get("timestamp", processing_time)

        # Calculate processing latency: how long since the event was created
        latency = processing_time - event_time

        # Which window does this event belong to?
        event_window_id = self.get_window_for_time(event_time)
        current_wall_time = processing_time
        current_wall_window_id = self.get_window_for_time(current_wall_time)

        # ── WINDOW MANAGEMENT ─────────────────────────────────────────────────

        # Is this a new window? (First event or window has changed)
        if self.current_window is None:
            # Start first window
            window_start = event_window_id * self.window_size
            self.current_window = WindowState(event_window_id, window_start, self.window_size)

        elif current_wall_window_id > self.current_window.window_id:
            # Time has moved past the current window — close it and start new one
            self._close_current_window()
            window_start = current_wall_window_id * self.window_size
            self.current_window = WindowState(current_wall_window_id, window_start, self.window_size)

        # ── LATE DATA DETECTION ───────────────────────────────────────────────

        # If the event's timestamp is earlier than current window start,
        # it's "late" — it arrived after its window closed
        if event_window_id < self.current_window.window_id:
            self.current_window.late_events += 1
            # In production: you'd either discard, or route to a side output
            # Here: we still include it in the current window (at-least-once semantics)

        # ── ADD EVENT TO CURRENT WINDOW ───────────────────────────────────────

        self.current_window.add_event(event, latency)
        self.total_events_processed += 1
        self.all_latencies.append(latency)

    def _close_current_window(self):
        """
        Close the current window, compute its stats, and save results.

        This is called when the time period for the current window expires.
        """
        if self.current_window is None or self.current_window.event_count == 0:
            return

        stats = self.current_window.compute_stats()
        self.completed_windows.append(stats)

        # Append to results file (JSONL format)
        with open(self.results_file, "a") as f:
            f.write(json.dumps(stats) + "\n")

        # Print window summary to console
        print(
            f"[Window {stats['window_id']:>4}] "
            f"Events: {stats['event_count']:>6,} | "
            f"Throughput: {stats['throughput_per_second']:>7.1f}/s | "
            f"p50: {stats['latency_p50_ms']:>6.1f}ms | "
            f"p99: {stats['latency_p99_ms']:>7.1f}ms | "
            f"Fraud: {stats['fraud_count']:>4} | "
            f"Late: {stats['late_events']}"
        )

    def flush(self):
        """Close the last window when processing ends."""
        self._close_current_window()

    def get_overall_stats(self) -> dict:
        """Compute aggregate statistics across all completed windows."""
        if not self.all_latencies:
            return {}

        sorted_lat = sorted(self.all_latencies)
        n = len(sorted_lat)

        def percentile(data, p):
            idx = min(int(p / 100 * n), n - 1)
            return data[idx]

        return {
            "total_events": self.total_events_processed,
            "total_windows": len(self.completed_windows),
            "overall_latency_p50_ms": round(percentile(sorted_lat, 50) * 1000, 2),
            "overall_latency_p95_ms": round(percentile(sorted_lat, 95) * 1000, 2),
            "overall_latency_p99_ms": round(percentile(sorted_lat, 99) * 1000, 2),
            "overall_latency_max_ms": round(max(sorted_lat) * 1000, 2),
            "avg_throughput_per_second": round(
                sum(w["throughput_per_second"] for w in self.completed_windows) /
                max(len(self.completed_windows), 1), 2
            ),
        }


def tail_file(filepath: str, processor: TumblingWindowProcessor, duration: int, poll_interval: float = 0.1):
    """
    Read a file as it grows — like `tail -f` in Linux.

    This is our simplified substitute for reading from Kafka.
    We watch the file for new lines and process them as they appear.

    Parameters:
        filepath: Path to the JSONL event file
        processor: The window processor
        duration: How long to run (seconds), 0 = until file stops growing
        poll_interval: How often to check for new data (seconds)
    """
    print(f"\n{'='*60}")
    print("CONSUMER STARTING")
    print(f"{'='*60}")
    print(f"  Input file:    {filepath}")
    print(f"  Window size:   {processor.window_size}s")
    print(f"  Duration:      {duration}s (0 = until producer stops)")
    print(f"{'='*60}")
    print(f"\n{'Window':>8} {'Events':>8} {'Throughput':>12} {'p50 lat':>10} {'p99 lat':>10} {'Fraud':>7} {'Late':>6}")
    print("-" * 70)

    start_time = time.time()
    last_active_time = start_time
    errors = 0

    # Wait for the file to exist (producer might not have started yet)
    wait_start = time.time()
    while not os.path.exists(filepath):
        if time.time() - wait_start > 30:
            print(f"ERROR: File not found after 30s: {filepath}")
            return
        print(f"Waiting for {filepath} to appear...")
        time.sleep(1)

    with open(filepath, "r") as f:
        while True:
            # Check if duration exceeded
            elapsed = time.time() - start_time
            if duration > 0 and elapsed >= duration:
                break

            line = f.readline()

            if not line:
                # No new data — file hasn't grown yet
                # Check if we've been idle too long (producer might be done)
                idle_time = time.time() - last_active_time
                if duration == 0 and idle_time > 5:
                    # Assume producer has finished
                    break
                time.sleep(poll_interval)
                continue

            line = line.strip()
            if not line:
                continue

            # Parse the JSON event
            try:
                event = json.loads(line)
                processor.process_event(event)
                last_active_time = time.time()
            except json.JSONDecodeError as e:
                errors += 1
                if errors <= 5:  # Only show first 5 errors to avoid spam
                    print(f"WARNING: Could not parse line: {e}")

    # Flush the last (possibly partial) window
    processor.flush()

    print(f"\n{'='*60}")
    print("CONSUMER FINISHED")


def simulate_crash_recovery(events_file: str, window_size: float, results_dir: str, crash_at_event: int = 500):
    """
    Demonstrate what happens when a consumer crashes and restarts.

    FAILURE HANDLING is a key topic in streaming systems.
    Real-world consumers crash due to:
    - Out-of-memory errors
    - Network timeouts
    - Bug in processing code
    - Machine failures

    RECOVERY STRATEGY (At-Least-Once Processing):
    1. Track the last successfully processed event ID (checkpoint)
    2. On restart, skip events up to the checkpoint
    3. Process from checkpoint onwards
    4. Some events may be processed twice (hence "at-least-once")

    This function simulates this pattern.
    """
    print(f"\n{'='*60}")
    print("FAILURE HANDLING SIMULATION")
    print(f"{'='*60}")
    print(f"  Simulating crash after {crash_at_event:,} events")
    print(f"  Then recovering from checkpoint and continuing...")

    checkpoint_file = os.path.join(results_dir, "checkpoint.json")
    events_processed = 0
    last_checkpoint_id = -1

    # PHASE 1: Process until simulated crash
    print(f"\nPhase 1: Processing events (will crash at event {crash_at_event:,})...")

    processor = TumblingWindowProcessor(window_size, results_dir + "/crash_test")

    try:
        with open(events_file, "r") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    event = json.loads(line)
                except json.JSONDecodeError:
                    continue

                if events_processed >= crash_at_event:
                    # Simulate crash
                    # Save checkpoint BEFORE crashing
                    with open(checkpoint_file, "w") as cp:
                        json.dump({"last_event_id": event.get("event_id", -1)}, cp)
                    print(f"  💥 SIMULATED CRASH at event {events_processed:,}")
                    print(f"     Checkpoint saved: event_id={event.get('event_id', -1)}")
                    last_checkpoint_id = event.get("event_id", -1)
                    break

                processor.process_event(event)
                events_processed += 1

    except Exception as e:
        print(f"  Error during phase 1: {e}")

    # PHASE 2: Restart and recover from checkpoint
    if os.path.exists(checkpoint_file):
        with open(checkpoint_file) as cp:
            checkpoint = json.load(cp)
        resume_from = checkpoint.get("last_event_id", -1)
        print(f"\nPhase 2: Recovering from checkpoint (event_id={resume_from})...")
        print(f"  Skipping events up to id={resume_from} (already processed)")

        processor2 = TumblingWindowProcessor(window_size, results_dir + "/recovered")
        recovered_count = 0

        with open(events_file, "r") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    event = json.loads(line)
                except json.JSONDecodeError:
                    continue

                # Skip events we already processed before the crash
                if event.get("event_id", -1) <= resume_from:
                    continue  # Already processed — skip

                processor2.process_event(event)
                recovered_count += 1

                # Only process a few hundred for the demo
                if recovered_count >= 200:
                    break

        processor2.flush()
        print(f"  ✓ Recovery complete: processed {recovered_count:,} events after checkpoint")

    print(f"\nFailure Handling Summary:")
    print(f"  - Events before crash:  {events_processed:,}")
    print(f"  - Checkpoint at:        event_id={last_checkpoint_id}")
    print(f"  - Semantics:            At-Least-Once (no events lost, some may replay)")
    print(f"  - Exactly-Once would require:  Idempotent writes or transactional sinks")


def main():
    parser = argparse.ArgumentParser(
        description="Stream processing consumer with windowing and latency tracking.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic usage — process events from producer.py output
  python consumer.py --input stream/events.jsonl --window 5

  # Run for exactly 60 seconds
  python consumer.py --input stream/events.jsonl --window 5 --duration 60

  # Simulate a crash and recovery
  python consumer.py --input stream/events.jsonl --window 5 --test-failure
        """
    )

    parser.add_argument("--input", type=str, default="stream/events.jsonl",
                        help="Input JSONL event file (default: stream/events.jsonl)")
    parser.add_argument("--window", type=float, default=5.0,
                        help="Window size in seconds (default: 5)")
    parser.add_argument("--duration", type=int, default=0,
                        help="How long to run in seconds, 0 = auto (default: 0)")
    parser.add_argument("--output", type=str, default="stream_results/",
                        help="Output directory for results (default: stream_results/)")
    parser.add_argument("--test-failure", action="store_true",
                        help="Run crash recovery simulation")

    args = parser.parse_args()

    if args.test_failure:
        # Run the crash recovery demo
        simulate_crash_recovery(
            events_file=args.input,
            window_size=args.window,
            results_dir=args.output,
            crash_at_event=500,
        )
        return

    # Normal operation: process stream
    processor = TumblingWindowProcessor(
        window_size=args.window,
        results_dir=args.output,
    )

    tail_file(
        filepath=args.input,
        processor=processor,
        duration=args.duration,
    )

    # Print and save overall statistics
    overall = processor.get_overall_stats()
    if overall:
        print("\n" + "="*60)
        print("OVERALL CONSUMER STATISTICS")
        print("="*60)
        for k, v in overall.items():
            print(f"  {k:<40} {v}")

        metrics_path = os.path.join(args.output, "consumer_metrics.json")
        os.makedirs(args.output, exist_ok=True)
        with open(metrics_path, "w") as f:
            json.dump(overall, f, indent=2)
        print(f"\nMetrics saved to: {metrics_path}")


if __name__ == "__main__":
    main()
