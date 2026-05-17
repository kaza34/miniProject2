#!/usr/bin/env python3
# validation.py
# Purpose: Validate the correctness of degraded service detection (Task 3)
# Focus: Verify that server error condition should be status_code >= 500, not == 500

import pandas as pd
from pathlib import Path

# Load dataset
DATA_PATH = Path("data") / "Comp3041J MiniProject 2 Dataset.csv"
df = pd.read_csv(DATA_PATH)

# Choose a service that contains various status codes (e.g., order-service)
service_example = "order-service"
service_df = df[df["service_name"] == service_example]

# Manually inspect first few rows, especially those with 500, 502, 503, 504
print(f"=== Validation for service: {service_example} ===")
print(service_df[["response_time_ms", "status_code", "error_type"]].head(20))

# Manually count records with status_code >= 500 (correct condition)
error_ge_500 = service_df[service_df["status_code"] >= 500].shape[0]
# Count records with status_code == 500 (incorrect condition originally used)
error_eq_500 = service_df[service_df["status_code"] == 500].shape[0]

print(f"Manual statistics for {service_example}:")
print(f"  Number of errors with status_code >= 500: {error_ge_500}")
print(f"  Number of errors with status_code == 500: {error_eq_500}")

# Pick a specific record with status_code = 502 (or another 5xx error)
sample_row = service_df[service_df["status_code"] == 502].iloc[0] if not service_df[service_df["status_code"] == 502].empty else None
if sample_row is not None:
    print("=== Single Record Validation Example ===")
    print(f"Request ID: {sample_row['request_id']}")
    print(f"Service: {sample_row['service_name']}")
    print(f"Status code: {sample_row['status_code']}")
    print(f"Response time: {sample_row['response_time_ms']} ms")
    print(f"Error type: {sample_row['error_type']}")
    print("\nExpected contribution:")
    print("  - Should NOT be counted as 'server error' if condition is ==500 → wrong")
    print("  - Should be counted as 'server error' if condition is >=500 → correct")

else:
    print("No record with status_code=502 found in the dataset. Try another 5xx error.")