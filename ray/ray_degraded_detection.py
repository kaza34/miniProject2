import ray
import pandas as pd
import os
import numpy as np

# =========================
# Ray initialization
# =========================
if ray.is_initialized():
    ray.shutdown()

ray.init(
    ignore_reinit_error=True,
    include_dashboard=False,
)

# =========================
# Load data
# =========================
def load_data(path):
    return pd.read_csv(path)

# =========================
# Split dataframe into chunks
# =========================
def split_dataframe(df, num_chunks):
    return np.array_split(df, num_chunks)


@ray.remote
def process_chunk(chunk):
    result = {}
    for _, row in chunk.iterrows():
        service = row["service_name"]
        if service not in result:
            result[service] = {"total": 0, "slow": 0, "error": 0, "timeout": 0}

        result[service]["total"] += 1

        # Slow request: response_time_ms > 800
        if row["response_time_ms"] > 800:
            result[service]["slow"] += 1

        # Server error: status_code == 500
        if row["status_code"] == 500:
            result[service]["error"] += 1

        # Timeout error: error_type == "Timeout" (string)
        error_type = row["error_type"]
        if isinstance(error_type, str) and error_type.strip() == "Timeout":
            result[service]["timeout"] += 1

    return result


# =========================
# Merge partial results from Ray
# =========================
def merge_results(partials):
    final = {}

    for part in partials:
        for service, stats in part.items():
            if service not in final:
                final[service] = stats
            else:
                final[service]["total"] += stats["total"]
                final[service]["slow"] += stats["slow"]
                final[service]["error"] += stats["error"]
                final[service]["timeout"] += stats["timeout"]

    return final


# =========================
# Detect degraded services
# =========================
def detect_degraded(stats):
    results = []

    for service, s in stats.items():
        total = s["total"]

        slow_rate = s["slow"] / total if total else 0
        error_rate = s["error"] / total if total else 0
        timeout_count = s["timeout"]

        reasons = []

        if slow_rate > 0.20:
            reasons.append("high slow request rate")

        if error_rate > 0.10:
            reasons.append("high server error rate")

        if timeout_count >= 5:
            reasons.append("repeated timeout errors")

        if reasons:
            results.append(f"{service}, {' | '.join(reasons)}")

    return results


# =========================
# Main function
# =========================
if __name__ == "__main__":
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

    input_path = os.path.join(
        base_dir,
        "data",
        "Comp3041J MiniProject 2 Dataset.csv"
    )

    output_path = os.path.join(
        base_dir,
        "output",
        "task3_degraded_services.txt"
    )

    # Load data
    df = load_data(input_path)

    # Split into chunks (adjustable)
    chunks = split_dataframe(df, num_chunks=4)

    # Parallel execution with Ray
    futures = [process_chunk.remote(chunk) for chunk in chunks]
    partial_results = ray.get(futures)

    # Merge results
    merged = merge_results(partial_results)

    # Detect degraded services
    degraded = detect_degraded(merged)

    # Write to TXT
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    with open(output_path, "w", encoding="utf-8") as f:
        for line in degraded:
            f.write(line + "\n")

    # Console output
    print("\n===== Degraded Services =====")
    for line in degraded:
        print(line)

    print(f"\n Result saved to: {output_path}")