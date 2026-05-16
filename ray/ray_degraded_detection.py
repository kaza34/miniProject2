import ray
import pandas as pd
import os
import numpy as np

# =========================
# Ray 初始化
# =========================
# ray.init(ignore_reinit_error=True)
import ray

if ray.is_initialized():
    ray.shutdown()

ray.init(
    local_mode=True,
    ignore_reinit_error=True,
    include_dashboard=False,
    _temp_dir="E:/ray_tmp"
)

# =========================
# 读取数据
# =========================
def load_data(path):
    return pd.read_csv(path)


# =========================
# 数据分块
# =========================
def split_dataframe(df, num_chunks):
    return np.array_split(df, num_chunks)


# =========================
# Ray 并行处理每个分块
# =========================
@ray.remote
def process_chunk(chunk):
    """
    对每个 chunk 做局部统计：
    - total requests
    - slow requests
    - server errors
    - timeout errors
    """

    result = {}

    for _, row in chunk.iterrows():

        service = row["service_name"]

        if service not in result:
            result[service] = {
                "total": 0,
                "slow": 0,
                "error": 0,
                "timeout": 0
            }

        result[service]["total"] += 1

        # ======= 兼容性判断（通用写法）=======
        row_str = str(row).lower()

        if "slow" in row_str:
            result[service]["slow"] += 1

        if "error" in row_str:
            result[service]["error"] += 1

        if "timeout" in row_str:
            result[service]["timeout"] += 1

    return result


# =========================
# 合并 Ray 结果
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
# Degraded Service 判定逻辑
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
            results.append(f"{service},{' | '.join(reasons)}")

    return results


# =========================
# 主函数
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

    # 读取数据
    df = load_data(input_path)

    # 分块（可调）
    chunks = split_dataframe(df, num_chunks=4)

    # Ray 并行执行
    futures = [process_chunk.remote(chunk) for chunk in chunks]
    partial_results = ray.get(futures)

    # 合并结果
    merged = merge_results(partial_results)

    # 判定 degraded service
    degraded = detect_degraded(merged)

    # =========================
    # 写入 TXT
    # =========================
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    with open(output_path, "w", encoding="utf-8") as f:
        for line in degraded:
            f.write(line + "\n")

    # 控制台输出
    print("\n===== Degraded Services =====")
    for line in degraded:
        print(line)

    print(f"\n✅ Result saved to: {output_path}")