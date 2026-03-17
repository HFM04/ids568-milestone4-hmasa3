"""
generate_data.py
----------------
This script creates a large synthetic (fake) dataset for testing our pipeline.
Think of it like generating fake customer transaction records for practice.

Why do we need this?
- We can't use real data (privacy concerns)
- We need to control exactly what the data looks like
- We need to be able to reproduce the EXACT same data every time (using a "seed")

What is a "seed"?
- Random number generators in computers aren't truly random
- If you give them the same starting number (the seed), they produce the same sequence
- This means anyone running this script with --seed 42 gets identical data to us

Usage:
    python generate_data.py --rows 1000 --output test_data/
    python generate_data.py --rows 10000000 --output data/ --seed 42
"""

import argparse       # For reading command-line arguments (like --rows 1000)
import os             # For creating folders/directories
import random         # For generating random numbers
import csv            # For writing CSV (spreadsheet-like) files
import time           # For measuring how long things take

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION
# Default values used if the user doesn't specify them on the command line
# ─────────────────────────────────────────────────────────────────────────────
DEFAULT_SEED = 42          # The "magic number" that makes randomness repeatable
DEFAULT_ROWS = 10_000_000  # 10 million rows (the assignment requirement)
DEFAULT_OUTPUT = "data/"   # Where to save the generated file

# These are the possible values for our categorical (text) columns
PRODUCT_CATEGORIES = ["Electronics", "Clothing", "Food", "Books", "Toys", "Sports"]
REGIONS = ["North", "South", "East", "West", "Central"]
PAYMENT_METHODS = ["Credit Card", "Debit Card", "PayPal", "Cash", "Gift Card"]


def generate_row(row_id: int) -> dict:
    """
    Generate a single row of fake transaction data.

    Parameters:
        row_id (int): A unique number for this row (like a customer ID)

    Returns:
        dict: A dictionary representing one transaction record

    What does a row look like?
        - transaction_id: unique identifier
        - user_id: which (fake) user made the purchase
        - product_category: what type of product
        - region: where the purchase happened
        - amount: how much they spent (in dollars)
        - quantity: how many items they bought
        - payment_method: how they paid
        - day_of_week: what day it was (0=Monday, 6=Sunday)
        - hour_of_day: what hour it was (0-23)
        - is_fraud: whether this was a fraudulent transaction (mostly False)
    """
    # Generate a random dollar amount between $1.00 and $999.99
    # round(..., 2) keeps it to 2 decimal places like real money
    amount = round(random.uniform(1.0, 999.99), 2)

    # Quantity between 1 and 20 items
    quantity = random.randint(1, 20)

    # Total spent = amount per item × quantity
    total = round(amount * quantity, 2)

    # Fraud is rare — only about 2% of transactions are fraudulent
    # random.random() gives a number between 0 and 1
    # If it's less than 0.02, we call it fraud
    is_fraud = random.random() < 0.02

    return {
        "transaction_id": row_id,
        "user_id": random.randint(1, 100_000),          # 100,000 possible users
        "product_category": random.choice(PRODUCT_CATEGORIES),
        "region": random.choice(REGIONS),
        "amount": amount,
        "quantity": quantity,
        "total": total,
        "payment_method": random.choice(PAYMENT_METHODS),
        "day_of_week": random.randint(0, 6),            # 0=Monday, 6=Sunday
        "hour_of_day": random.randint(0, 23),           # Hour in 24-hour format
        "is_fraud": is_fraud,
    }


def generate_dataset(num_rows: int, output_dir: str, seed: int) -> str:
    """
    Generate the full dataset and save it as a CSV file.

    Parameters:
        num_rows (int): How many rows to generate
        output_dir (str): Which folder to save the file in
        seed (int): The random seed for reproducibility

    Returns:
        str: The path to the saved file
    """
    # Set the random seed FIRST — this is the key to reproducibility
    # Every call to random.random() or random.randint() after this will be
    # deterministic (same results every time with the same seed)
    random.seed(seed)

    # Create the output directory if it doesn't already exist
    # exist_ok=True means "don't crash if the folder already exists"
    os.makedirs(output_dir, exist_ok=True)

    output_path = os.path.join(output_dir, "transactions.csv")

    print(f"Generating {num_rows:,} rows with seed={seed}...")
    print(f"Output file: {output_path}")

    start_time = time.time()

    # Open the file for writing
    # newline='' is required for csv.writer to work correctly on Windows
    with open(output_path, "w", newline="") as f:
        # Get the column names from our first row
        first_row = generate_row(0)
        fieldnames = list(first_row.keys())

        # csv.DictWriter writes dictionaries as CSV rows
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()  # Write the column names at the top

        # Write data in chunks to avoid using too much memory at once
        # Instead of keeping all 10M rows in memory, we write 10,000 at a time
        CHUNK_SIZE = 10_000

        for chunk_start in range(0, num_rows, CHUNK_SIZE):
            chunk_end = min(chunk_start + CHUNK_SIZE, num_rows)

            # Generate a chunk of rows
            rows = [generate_row(i) for i in range(chunk_start, chunk_end)]
            writer.writerows(rows)

            # Print progress every 500,000 rows
            if chunk_start % 500_000 == 0 and chunk_start > 0:
                elapsed = time.time() - start_time
                pct = (chunk_start / num_rows) * 100
                print(f"  Progress: {chunk_start:,} / {num_rows:,} rows ({pct:.1f}%) — {elapsed:.1f}s elapsed")

    elapsed = time.time() - start_time
    file_size_mb = os.path.getsize(output_path) / (1024 * 1024)

    print(f"\n✓ Done! Generated {num_rows:,} rows in {elapsed:.2f} seconds")
    print(f"  File size: {file_size_mb:.1f} MB")
    print(f"  Location: {output_path}")

    return output_path


def main():
    """
    Main entry point — reads command-line arguments and runs the generator.

    This function uses argparse, which lets users customize behavior like:
        python generate_data.py --rows 1000 --seed 99 --output my_data/
    """
    parser = argparse.ArgumentParser(
        description="Generate synthetic transaction data for the MLOps pipeline.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Quick test with 1,000 rows
  python generate_data.py --rows 1000 --output test_data/

  # Full 10M row dataset for benchmarking
  python generate_data.py --rows 10000000 --output data/ --seed 42

  # Same command twice → identical output (reproducibility test)
  python generate_data.py --rows 100 --seed 42 --output run1/
  python generate_data.py --rows 100 --seed 42 --output run2/
        """
    )

    parser.add_argument(
        "--rows",
        type=int,
        default=DEFAULT_ROWS,
        help=f"Number of rows to generate (default: {DEFAULT_ROWS:,})"
    )
    parser.add_argument(
        "--output",
        type=str,
        default=DEFAULT_OUTPUT,
        help=f"Output directory (default: '{DEFAULT_OUTPUT}')"
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=DEFAULT_SEED,
        help=f"Random seed for reproducibility (default: {DEFAULT_SEED})"
    )

    args = parser.parse_args()

    # Run the data generation
    generate_dataset(
        num_rows=args.rows,
        output_dir=args.output,
        seed=args.seed,
    )


# This block only runs when you execute this file directly
# It does NOT run if another script imports this file
if __name__ == "__main__":
    main()
