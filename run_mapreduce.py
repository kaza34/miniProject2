#!/usr/bin/env python3

import subprocess
import sys
from pathlib import Path

DATA_FILE = Path("data") / "Comp3041J MiniProject 2 Dataset.csv"
MAPREDUCE_DIR = Path("mapreduce")
OUTPUT_DIR = Path("output")

JOBS = [
    ("Output 1: Request Count by Service", "request_count_mapper.py", "request_count_reducer.py", "request_count.txt"),
    ("Output 2: Server Error Count (status=500) by Service", "error_count_mapper.py", "error_count_reducer.py", "error_count.txt"),
    ("Output 3: Top 10 Slow Endpoints (response_time > 800 ms)", "slow_endpoint_mapper.py", "slow_endpoint_reducer.py", "slow_endpoint.txt"),
]

def ensure_output_dir():
    """Create output directory if it doesn't exist."""
    OUTPUT_DIR.mkdir(exist_ok=True)

def run_mapreduce(mapper_script, reducer_script, input_file):
    """
    Simulate MapReduce pipeline:
    cat input | mapper | sort | reducer
    Returns the reducer's standard output as a string.
    """
    # 1. Read CSV file and skip the header
    with open(input_file, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    data_lines = lines[1:] if lines else []

    # 2. Start mapper subprocess
    mapper_proc = subprocess.Popen(
        [sys.executable, str(MAPREDUCE_DIR / mapper_script)],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        text=True,
        bufsize=1
    )
    mapper_stdout, _ = mapper_proc.communicate(input=''.join(data_lines))

    # 3. Sort (simulate shuffle)
    sorted_lines = sorted(mapper_stdout.splitlines(keepends=True))

    # 4. Start reducer subprocess
    reducer_proc = subprocess.Popen(
        [sys.executable, str(MAPREDUCE_DIR / reducer_script)],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        text=True
    )
    reducer_stdout, _ = reducer_proc.communicate(input=''.join(sorted_lines))

    return reducer_stdout.strip()

def main():
    # Check if data file exists
    if not DATA_FILE.exists():
        error_msg = f"Error: Data file not found at {DATA_FILE}\nPlease ensure Comp3041J MiniProject 2 Dataset.csv exists in the data directory."
        print(error_msg)
        return

    # Create output directory
    ensure_output_dir()

    # Run the three jobs sequentially, saving results to files only (no console output)
    for title, mapper, reducer, out_file in JOBS:
        try:
            result = run_mapreduce(mapper, reducer, DATA_FILE)
            output_path = OUTPUT_DIR / out_file
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(result if result else "(no output)")
        except Exception as e:
            # Write error information to the corresponding file if an exception occurs
            output_path = OUTPUT_DIR / out_file
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(f"Runtime error: {e}")

    # Optional: print a simple completion message to the console
    # print("All results saved to output folder.")

if __name__ == "__main__":
    main()