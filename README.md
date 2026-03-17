# Milestone 4: Distributed & Streaming Pipeline

**MLOps Course – Module 5**  
**Author:** [Your Name] | **NetID:** [your_netid]

---

## What This Project Does

This project builds two things:

1. **A distributed batch processing pipeline** that reads 10 million synthetic transaction records and engineers ML-ready features using PySpark (the Python interface to Apache Spark).

2. **A streaming pipeline (bonus)** that generates and processes a continuous stream of transaction events in real time, using tumbling windows to compute statistics and latencies.

---

## Prerequisites

You need **Python 3.8+** and **Java 8 or 11** installed.

Check your versions:
```bash
python --version    # Should be 3.8 or higher
java -version       # Should be 1.8 or 11
```

> **Why Java?** Apache Spark runs on the Java Virtual Machine (JVM). PySpark is just a Python wrapper around Spark — Java must be installed for it to work.

---

## Installation

### Step 1 — Clone the Repository
```bash
git clone https://github.com/[your-username]/ids568-milestone4-[netid].git
cd ids568-milestone4-[netid]
```

### Step 2 — Install Python Dependencies
```bash
pip install -r requirements.txt
```

This installs:
- `pyspark` — the distributed processing framework
- `pandas` — for small-scale data manipulation
- `matplotlib` — for generating performance charts

---

## Quick Start (Validate Before Running Full Scale)

**Always test with small data first!** Large-scale bugs are much harder to debug.

```bash
# 1. Generate a small test dataset (1,000 rows — fast)
python generate_data.py --rows 1000 --output test_data/

# 2. Run the pipeline on the small dataset
python pipeline.py --input test_data/ --output test_output/ --workers 2

# 3. Verify output exists
ls test_output/
```

If this works without errors, you're ready for the full run.

---

## Full Benchmark Run

### Step 1 — Generate the Full Dataset (10M rows)

```bash
python generate_data.py --rows 10000000 --output data/ --seed 42
```

This takes approximately 3–8 minutes and creates a `data/transactions.csv` file (~800 MB).

### Step 2 — Run Local Baseline (1 worker, simulates single-machine processing)

```bash
python pipeline.py --input data/ --output output_local/ --workers 1
```

Record the total runtime from the output.

### Step 3 — Run Distributed Configuration (multiple workers)

```bash
# 4 workers
python pipeline.py --input data/ --output output_4w/ --workers 4

# 8 workers (if your machine has 8+ cores)
python pipeline.py --input data/ --output output_8w/ --workers 8
```

### Step 4 — Compare Results

Each run saves a `pipeline_metrics.json` file in the output directory. Compare runtimes to see the speedup from parallelism.

---

## Reproducibility Verification

The `--seed` flag ensures identical output every time. To verify:

```bash
# Run twice with same seed
python generate_data.py --rows 100 --seed 42 --output run1/
python generate_data.py --rows 100 --seed 42 --output run2/

# Compare — should show no differences
diff run1/transactions.csv run2/transactions.csv
echo "Exit code 0 = files are identical ✓"
```

---

## Streaming Pipeline (Bonus)

The streaming pipeline requires two terminals running simultaneously.

### Terminal 1 — Start the Producer
```bash
# Steady stream at 100 events/second for 60 seconds
python producer.py --rate 100 --duration 60 --pattern steady

# Or bursty traffic to test backpressure
python producer.py --rate 500 --duration 120 --pattern bursty
```

### Terminal 2 — Start the Consumer
```bash
# Process events in 5-second windows
python consumer.py --input stream/events.jsonl --window 5
```

### Test Crash Recovery
```bash
# First generate some events
python producer.py --rate 200 --duration 30 --output stream/events.jsonl

# Then simulate a crash and recovery
python consumer.py --input stream/events.jsonl --window 5 --test-failure
```

---

## Output Files

After running the pipeline, you'll find:

```
output_local/
├── featured_transactions/     # Parquet files with engineered features
├── region_stats/              # CSV: revenue and fraud by region
├── category_stats/            # CSV: revenue by product category
├── time_stats/                # CSV: activity by time of day
├── user_stats/                # CSV: per-user spending summary
└── pipeline_metrics.json      # Timing metrics for the report
```

---

## Repository Structure

```
ids568-milestone4-[netid]/
├── pipeline.py             # Main distributed pipeline (PySpark)
├── generate_data.py        # Synthetic data generator
├── producer.py             # Streaming event generator (bonus)
├── consumer.py             # Streaming processor with windowing (bonus)
├── README.md               # This file
├── REPORT.md               # Performance analysis and architecture notes
├── STREAMING_REPORT.md     # Streaming analysis (bonus)
└── requirements.txt        # Python dependencies
```

---

## Troubleshooting

| Problem | Solution |
|---|---|
| `java.lang.RuntimeException: Java not found` | Install Java 11: `sudo apt install openjdk-11-jdk` |
| `ModuleNotFoundError: No module named 'pyspark'` | Run `pip install -r requirements.txt` |
| Pipeline crashes on 10M rows | Start with `--rows 100000` to debug, then scale up |
| Consumer shows no output | Make sure producer is writing to same file path |
