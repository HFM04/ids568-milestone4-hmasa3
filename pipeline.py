"""
pipeline.py
-----------
This is the main distributed data processing pipeline for Milestone 4.

WHAT IS A PIPELINE?
A pipeline is a series of processing steps where the output of one step
feeds into the next — like an assembly line in a factory.

WHY PYSPARK?
PySpark is the Python interface for Apache Spark — a framework that splits
work across many "worker" machines (or CPU cores) in parallel. Instead of
one machine processing all 10M rows one-by-one, Spark breaks the data into
"partitions" (chunks) and processes them simultaneously.

Think of it like this:
- Without Spark: 1 worker reads and processes 10,000,000 rows (slow)
- With Spark: 8 workers each process 1,250,000 rows in parallel (much faster)

WHAT DOES THIS PIPELINE DO?
1. Reads the synthetic transaction CSV data
2. Cleans and validates the data
3. Engineers new features (creates new columns from existing ones)
4. Aggregates data (computes statistics per user, region, etc.)
5. Writes the results to disk in a compressed, efficient format

Usage:
    python pipeline.py --input data/ --output output/
    python pipeline.py --input data/ --output output/ --mode local --partitions 8
"""

import argparse
import os
import time
import json

# PySpark imports
# SparkSession is the "entry point" — you need it to do anything with Spark
from pyspark.sql import SparkSession

# These are Spark's built-in column functions (like Excel formulas for big data)
from pyspark.sql import functions as F

# Data types — tells Spark what kind of data each column holds
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, FloatType, StringType, BooleanType, DoubleType
)

# ─────────────────────────────────────────────────────────────────────────────
# SCHEMA DEFINITION
# Defining the schema (column types) explicitly is better than letting Spark
# guess. It's faster and prevents type errors.
# ─────────────────────────────────────────────────────────────────────────────
TRANSACTION_SCHEMA = StructType([
    StructField("transaction_id",  IntegerType(), nullable=False),
    StructField("user_id",         IntegerType(), nullable=False),
    StructField("product_category", StringType(), nullable=True),
    StructField("region",          StringType(),  nullable=True),
    StructField("amount",          DoubleType(),  nullable=True),
    StructField("quantity",        IntegerType(), nullable=True),
    StructField("total",           DoubleType(),  nullable=True),
    StructField("payment_method",  StringType(),  nullable=True),
    StructField("day_of_week",     IntegerType(), nullable=True),
    StructField("hour_of_day",     IntegerType(), nullable=True),
    StructField("is_fraud",        StringType(),  nullable=True),  # CSV stores as "True"/"False"
])


def create_spark_session(app_name: str, num_workers: int) -> SparkSession:
    """
    Create and configure a SparkSession.

    The SparkSession is the gateway to all Spark functionality.
    It's like starting up Excel before you can work with spreadsheets.

    Parameters:
        app_name (str): A human-readable name for this Spark job
        num_workers (int): How many parallel workers (CPU cores) to use

    Returns:
        SparkSession: The configured Spark session
    """
    print(f"\n{'='*60}")
    print(f"Starting Spark session: '{app_name}'")
    print(f"Workers (parallel cores): {num_workers}")
    print(f"{'='*60}\n")

    spark = (
        SparkSession.builder
        .appName(app_name)
        # local[N] means run locally using N CPU cores
        # local[1] = single-threaded (baseline comparison)
        # local[*] = use all available cores
        .master(f"local[{num_workers}]")

        # Memory settings — how much RAM each executor (worker) can use
        .config("spark.executor.memory", "2g")
        .config("spark.driver.memory", "2g")

        # This controls how many partitions Spark creates after a shuffle
        # A "shuffle" happens when data needs to be reorganized across workers
        # (e.g., when grouping by a column, data for the same group must
        #  be on the same machine)
        .config("spark.sql.shuffle.partitions", str(num_workers * 4))

        # Enable adaptive query execution — Spark auto-tunes itself
        .config("spark.sql.adaptive.enabled", "true")

        # Reduce logging noise so we can see our print statements
        .config("spark.ui.showConsoleProgress", "false")

        .getOrCreate()
    )

    # Set log level to only show errors (not verbose INFO messages)
    spark.sparkContext.setLogLevel("ERROR")

    return spark


def load_data(spark: SparkSession, input_path: str, num_partitions: int):
    """
    Load the CSV data into a Spark DataFrame.

    A DataFrame in Spark is like a very large spreadsheet that lives
    across multiple machines. Each row is a transaction record.

    Parameters:
        spark: The active SparkSession
        input_path: Where the CSV file(s) are located
        num_partitions: How many chunks to split the data into

    Returns:
        DataFrame: The loaded data
    """
    print(f"Loading data from: {input_path}")
    start = time.time()

    df = (
        spark.read
        .option("header", "true")      # First row is column names
        .option("inferSchema", "false") # Don't guess types — we provide them
        .schema(TRANSACTION_SCHEMA)
        .csv(input_path)
        # Repartition splits data into equal chunks for parallel processing
        # More partitions = more parallelism, but too many = overhead
        .repartition(num_partitions)
    )

    # cache() keeps the DataFrame in memory so we don't re-read the file later
    # This speeds up subsequent operations
    df.cache()

    # count() forces Spark to actually load the data (Spark is "lazy" — it
    # waits until you ask for results before doing work)
    row_count = df.count()

    elapsed = time.time() - start
    print(f"  ✓ Loaded {row_count:,} rows in {elapsed:.2f}s")
    print(f"  ✓ Partitions: {df.rdd.getNumPartitions()}")

    return df, row_count


def clean_data(df):
    """
    Clean and validate the data.

    Real-world data is messy. This step:
    - Removes rows with missing critical values
    - Converts string "True"/"False" to actual booleans
    - Filters out impossible values (e.g., negative amounts)

    Parameters:
        df: Input Spark DataFrame

    Returns:
        DataFrame: Cleaned data
    """
    print("\nStep 1: Cleaning data...")
    start = time.time()

    cleaned = (
        df
        # Remove rows where critical fields are null (missing)
        .dropna(subset=["transaction_id", "user_id", "amount", "total"])

        # Convert the is_fraud column from string "True"/"False" to boolean
        # F.col("is_fraud") refers to the "is_fraud" column
        # == "True" creates a new boolean column
        .withColumn("is_fraud", F.col("is_fraud") == "True")

        # Remove nonsensical values
        .filter(F.col("amount") > 0)           # Can't have negative/zero prices
        .filter(F.col("quantity") > 0)         # Must have at least 1 item
        .filter(F.col("total") > 0)            # Total must be positive
        .filter(F.col("hour_of_day").between(0, 23))   # Valid hour range
        .filter(F.col("day_of_week").between(0, 6))    # Valid day range
    )

    count_before = df.count()
    count_after = cleaned.count()
    rows_removed = count_before - count_after
    elapsed = time.time() - start

    print(f"  ✓ Removed {rows_removed:,} invalid rows")
    print(f"  ✓ {count_after:,} rows remain after cleaning ({elapsed:.2f}s)")

    return cleaned


def engineer_features(df):
    """
    Feature Engineering — the heart of the pipeline.

    "Feature engineering" means creating new columns (features) from
    existing data. These new features often help machine learning models
    make better predictions.

    Example: Instead of just knowing the "hour" of a transaction, we
    create "is_peak_hour" to flag busy shopping times.

    Parameters:
        df: Cleaned Spark DataFrame

    Returns:
        DataFrame: DataFrame with new feature columns added
    """
    print("\nStep 2: Engineering features...")
    start = time.time()

    featured = (
        df

        # ── TIME-BASED FEATURES ──────────────────────────────────────────────

        # Is this transaction on a weekend? (day 5=Saturday, 6=Sunday)
        .withColumn(
            "is_weekend",
            F.col("day_of_week").isin([5, 6])
        )

        # Is this during peak shopping hours? (10am-8pm)
        .withColumn(
            "is_peak_hour",
            F.col("hour_of_day").between(10, 20)
        )

        # Which part of the day?
        # F.when() works like an IF/ELSE statement:
        # IF hour < 6 THEN "Night" ELSEIF hour < 12 THEN "Morning" etc.
        .withColumn(
            "time_of_day",
            F.when(F.col("hour_of_day") < 6, "Night")
             .when(F.col("hour_of_day") < 12, "Morning")
             .when(F.col("hour_of_day") < 18, "Afternoon")
             .otherwise("Evening")
        )

        # ── PRICE-BASED FEATURES ─────────────────────────────────────────────

        # Price tier: categorize how expensive the transaction is
        .withColumn(
            "price_tier",
            F.when(F.col("amount") < 25, "Budget")
             .when(F.col("amount") < 100, "Mid-Range")
             .when(F.col("amount") < 500, "Premium")
             .otherwise("Luxury")
        )

        # High value flag: transactions over $500 total
        .withColumn(
            "is_high_value",
            F.col("total") > 500
        )

        # Discount potential: estimated discount based on quantity
        # Buying more items = bigger potential bulk discount
        .withColumn(
            "bulk_purchase",
            F.col("quantity") >= 10
        )

        # ── DERIVED NUMERIC FEATURES ─────────────────────────────────────────

        # Amount per item (verify the total matches)
        # This helps catch data integrity issues
        .withColumn(
            "amount_per_item",
            F.round(F.col("total") / F.col("quantity"), 2)
        )

        # Log of total (useful for ML models — reduces the impact of
        # very large values, making the distribution more "normal")
        .withColumn(
            "log_total",
            F.log(F.col("total") + 1)  # +1 avoids log(0) which is undefined
        )

        # ── RISK FEATURES (useful for fraud detection) ────────────────────────

        # Late night transactions are higher risk for fraud
        .withColumn(
            "is_late_night",
            F.col("hour_of_day").isin([0, 1, 2, 3, 4])
        )

        # Combined risk flag: late night + high value = suspicious
        .withColumn(
            "high_risk_flag",
            F.col("is_late_night") & F.col("is_high_value")
        )
    )

    elapsed = time.time() - start
    new_cols = len(featured.columns) - len(df.columns)
    print(f"  ✓ Added {new_cols} new feature columns ({elapsed:.2f}s)")
    print(f"  ✓ Total columns: {len(featured.columns)}")

    return featured


def compute_aggregations(df):
    """
    Compute summary statistics by grouping data.

    Aggregations answer questions like:
    - "What is the average transaction amount per region?"
    - "Which product category generates the most revenue?"
    - "Which users have the highest fraud rate?"

    This step involves a "shuffle" — Spark must move rows with the same
    group key (e.g., same region) to the same worker. This is the most
    expensive operation in distributed processing.

    Parameters:
        df: Feature-engineered Spark DataFrame

    Returns:
        Tuple of aggregated DataFrames
    """
    print("\nStep 3: Computing aggregations...")
    start = time.time()

    # ── AGGREGATION 1: By Region ──────────────────────────────────────────────
    # This requires a shuffle: all "North" rows go to one worker,
    # all "South" rows to another, etc.
    region_stats = (
        df
        .groupBy("region")
        .agg(
            F.count("*").alias("transaction_count"),
            F.round(F.avg("total"), 2).alias("avg_transaction_value"),
            F.round(F.sum("total"), 2).alias("total_revenue"),
            F.round(F.avg("quantity"), 2).alias("avg_quantity"),
            F.sum(F.col("is_fraud").cast("int")).alias("fraud_count"),
            F.round(
                F.avg(F.col("is_fraud").cast("int")) * 100, 4
            ).alias("fraud_rate_pct"),
        )
        .orderBy("total_revenue", ascending=False)
    )

    # ── AGGREGATION 2: By Product Category ───────────────────────────────────
    category_stats = (
        df
        .groupBy("product_category")
        .agg(
            F.count("*").alias("transaction_count"),
            F.round(F.avg("total"), 2).alias("avg_transaction_value"),
            F.round(F.sum("total"), 2).alias("total_revenue"),
            F.round(F.avg("quantity"), 2).alias("avg_quantity"),
            F.countDistinct("user_id").alias("unique_buyers"),
        )
        .orderBy("total_revenue", ascending=False)
    )

    # ── AGGREGATION 3: By Time of Day ─────────────────────────────────────────
    time_stats = (
        df
        .groupBy("time_of_day")
        .agg(
            F.count("*").alias("transaction_count"),
            F.round(F.avg("total"), 2).alias("avg_transaction_value"),
            F.round(F.sum("total"), 2).alias("total_revenue"),
        )
        .orderBy("transaction_count", ascending=False)
    )

    # ── AGGREGATION 4: User-level stats (fraud analysis) ─────────────────────
    # This is the heaviest aggregation — 100,000 unique users
    user_stats = (
        df
        .groupBy("user_id")
        .agg(
            F.count("*").alias("total_transactions"),
            F.round(F.sum("total"), 2).alias("total_spent"),
            F.round(F.avg("total"), 2).alias("avg_transaction"),
            F.sum(F.col("is_fraud").cast("int")).alias("fraud_count"),
        )
        # Only include users with multiple transactions (more interesting)
        .filter(F.col("total_transactions") > 1)
        .orderBy("total_spent", ascending=False)
    )

    elapsed = time.time() - start
    print(f"  ✓ Computed 4 aggregation tables in {elapsed:.2f}s")

    return region_stats, category_stats, time_stats, user_stats


def write_results(df_featured, region_stats, category_stats, time_stats, user_stats, output_dir: str):
    """
    Write all results to disk in Parquet format.

    WHY PARQUET INSTEAD OF CSV?
    - Parquet is a columnar format (data is stored column-by-column, not row-by-row)
    - Reading just a few columns is much faster (no need to read entire rows)
    - Built-in compression: typically 5-10x smaller than CSV
    - Preserves data types (no need to re-parse strings into numbers)
    - Industry standard for big data storage

    Parameters:
        df_featured: The feature-engineered full dataset
        region_stats, category_stats, time_stats, user_stats: Aggregation results
        output_dir: Where to write files
    """
    print(f"\nStep 4: Writing results to {output_dir}...")
    start = time.time()

    os.makedirs(output_dir, exist_ok=True)

    # Write each output with overwrite mode (replace if already exists)
    # coalesce(1) combines all partitions into a single file for easy reading
    # (In production you'd keep multiple partitions for parallel reads)

    print("  Writing featured transactions...")
    (
        df_featured
        .coalesce(4)  # Combine into 4 files for balance of size and readability
        .write
        .mode("overwrite")
        .parquet(os.path.join(output_dir, "featured_transactions"))
    )

    print("  Writing region statistics...")
    region_stats.coalesce(1).write.mode("overwrite").csv(
        os.path.join(output_dir, "region_stats"), header=True
    )

    print("  Writing category statistics...")
    category_stats.coalesce(1).write.mode("overwrite").csv(
        os.path.join(output_dir, "category_stats"), header=True
    )

    print("  Writing time-of-day statistics...")
    time_stats.coalesce(1).write.mode("overwrite").csv(
        os.path.join(output_dir, "time_stats"), header=True
    )

    print("  Writing user statistics...")
    user_stats.coalesce(1).write.mode("overwrite").csv(
        os.path.join(output_dir, "user_stats"), header=True
    )

    elapsed = time.time() - start
    print(f"  ✓ All results written in {elapsed:.2f}s")


def print_sample_results(region_stats, category_stats):
    """Print a quick preview of results to the console."""
    print("\n" + "="*60)
    print("SAMPLE RESULTS")
    print("="*60)

    print("\n--- Revenue by Region ---")
    region_stats.show(truncate=False)

    print("\n--- Revenue by Product Category ---")
    category_stats.show(truncate=False)


def run_pipeline(input_path: str, output_path: str, num_workers: int, num_partitions: int):
    """
    Orchestrates the full pipeline from start to finish.

    This is the main function that ties everything together:
    Load → Clean → Feature Engineer → Aggregate → Write

    Parameters:
        input_path: Where the input CSV data is
        output_path: Where to write results
        num_workers: Number of parallel Spark workers (CPU cores)
        num_partitions: How many chunks to split data into
    """
    pipeline_start = time.time()

    # Step 0: Start Spark
    spark = create_spark_session(
        app_name="MLOps-Milestone4-Pipeline",
        num_workers=num_workers
    )

    # Collect metrics for the report
    metrics = {
        "num_workers": num_workers,
        "num_partitions": num_partitions,
        "steps": {}
    }

    try:
        # Step 1: Load
        t0 = time.time()
        df, row_count = load_data(spark, input_path, num_partitions)
        metrics["row_count"] = row_count
        metrics["steps"]["load"] = round(time.time() - t0, 2)

        # Step 2: Clean
        t0 = time.time()
        df_clean = clean_data(df)
        metrics["steps"]["clean"] = round(time.time() - t0, 2)

        # Step 3: Feature engineering
        t0 = time.time()
        df_featured = engineer_features(df_clean)
        metrics["steps"]["feature_engineering"] = round(time.time() - t0, 2)

        # Step 4: Aggregations
        t0 = time.time()
        region_stats, category_stats, time_stats, user_stats = compute_aggregations(df_featured)
        metrics["steps"]["aggregations"] = round(time.time() - t0, 2)

        # Step 5: Write output
        t0 = time.time()
        write_results(df_featured, region_stats, category_stats, time_stats, user_stats, output_path)
        metrics["steps"]["write"] = round(time.time() - t0, 2)

        # Print sample results
        print_sample_results(region_stats, category_stats)

    finally:
        # Always stop Spark cleanly, even if there's an error
        spark.stop()

    total_time = round(time.time() - pipeline_start, 2)
    metrics["total_runtime_seconds"] = total_time

    print(f"\n{'='*60}")
    print("PIPELINE COMPLETE")
    print(f"{'='*60}")
    print(f"Total runtime: {total_time:.2f} seconds")
    print(f"Rows processed: {row_count:,}")
    if total_time > 0:
        print(f"Throughput: {row_count / total_time:,.0f} rows/second")
    print(f"\nStep breakdown:")
    for step, secs in metrics["steps"].items():
        print(f"  {step:<25} {secs:>7.2f}s")

    # Save metrics JSON for the report
    metrics_path = os.path.join(output_path, "pipeline_metrics.json")
    os.makedirs(output_path, exist_ok=True)
    with open(metrics_path, "w") as f:
        json.dump(metrics, f, indent=2)
    print(f"\nMetrics saved to: {metrics_path}")

    return metrics


def main():
    """Parse command-line arguments and run the pipeline."""
    parser = argparse.ArgumentParser(
        description="Distributed feature engineering pipeline using PySpark.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Baseline: single-threaded (simulates local execution)
  python pipeline.py --input data/ --output output_local/ --workers 1

  # Distributed: use all available CPU cores
  python pipeline.py --input data/ --output output_dist/ --workers 8

  # Small test run first (always validate before scaling up!)
  python generate_data.py --rows 1000 --output test_data/
  python pipeline.py --input test_data/ --output test_output/
        """
    )

    parser.add_argument("--input", type=str, default="data/",
                        help="Input directory containing CSV files")
    parser.add_argument("--output", type=str, default="output/",
                        help="Output directory for results")
    parser.add_argument("--workers", type=int, default=4,
                        help="Number of parallel workers/CPU cores (default: 4)")
    parser.add_argument("--partitions", type=int, default=None,
                        help="Number of data partitions (default: workers * 4)")

    args = parser.parse_args()

    # Default partitions = 4x the number of workers
    # This gives each worker 4 chunks to process, which balances overhead
    partitions = args.partitions or (args.workers * 4)

    run_pipeline(
        input_path=args.input,
        output_path=args.output,
        num_workers=args.workers,
        num_partitions=partitions,
    )


if __name__ == "__main__":
    main()
