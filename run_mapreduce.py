# #!/usr/bin/env python3
# # run_mapreduce.py - 在 Windows PyCharm 中测试三个 MapReduce 作业
#
# import subprocess
# import sys
# import os
# from pathlib import Path
#
# # 配置文件路径
# DATA_FILE = Path("data") / "Comp3041J MiniProject 2 Dataset.csv"
# MAPREDUCE_DIR = Path("mapreduce")
# JOBS = [
#     ("Output 1: Request Count by Service", "request_count_mapper.py", "request_count_reducer.py"),
#     ("Output 2: Server Error Count (status=500) by Service", "error_count_mapper.py", "error_count_reducer.py"),
#     ("Output 3: Top 10 Slow Endpoints (response_time > 800 ms)", "slow_endpoint_mapper.py", "slow_endpoint_reducer.py"),
# ]
#
#
# def run_mapreduce(mapper_script, reducer_script, input_file):
#     """
#     模拟 MapReduce pipeline:
#     cat input | mapper | sort | reducer
#     """
#     # 1. 读取 CSV 文件并跳过表头
#     with open(input_file, 'r', encoding='utf-8') as f:
#         lines = f.readlines()
#     # 跳过第一行表头
#     data_lines = lines[1:] if lines else []
#
#     # 2. 启动 mapper 子进程
#     mapper_proc = subprocess.Popen(
#         [sys.executable, str(MAPREDUCE_DIR / mapper_script)],
#         stdin=subprocess.PIPE,
#         stdout=subprocess.PIPE,
#         text=True,
#         bufsize=1
#     )
#     # 将数据行写入 mapper 的 stdin
#     mapper_stdout, _ = mapper_proc.communicate(input=''.join(data_lines))
#
#     # 3. 对 mapper 输出进行排序（模拟 shuffle）
#     sorted_lines = sorted(mapper_stdout.splitlines(keepends=True))
#
#     # 4. 启动 reducer 子进程
#     reducer_proc = subprocess.Popen(
#         [sys.executable, str(MAPREDUCE_DIR / reducer_script)],
#         stdin=subprocess.PIPE,
#         stdout=subprocess.PIPE,
#         text=True
#     )
#     reducer_stdout, _ = reducer_proc.communicate(input=''.join(sorted_lines))
#
#     return reducer_stdout.strip()
#
#
# def main():
#     if not DATA_FILE.exists():
#         print(f"错误：找不到数据文件 {DATA_FILE}")
#         print("请确保 data 目录下存在 Comp3041J MiniProject 2 Dataset.csv")
#         return
#
#     for title, mapper, reducer in JOBS:
#         print("=" * 60)
#         print(title)
#         print("-" * 60)
#         try:
#             result = run_mapreduce(mapper, reducer, DATA_FILE)
#             print(result if result else "(无输出)")
#         except Exception as e:
#             print(f"运行出错：{e}")
#         print()
#
#
# if __name__ == "__main__":
#     main()
# !/usr/bin/env python3
# run_mapreduce.py - 安静模式，只将三个 MapReduce 作业的输出保存到 output 文件夹

import subprocess
import sys
from pathlib import Path


DATA_FILE = Path("data") / "Comp3041J MiniProject 2 Dataset.csv"
MAPREDUCE_DIR = Path("mapreduce")
OUTPUT_DIR = Path("output")



JOBS = [
    ("Output 1: Request Count by Service", "request_count_mapper.py", "request_count_reducer.py","request_count.txt"),
    ("Output 2: Server Error Count (status=500) by Service", "error_count_mapper.py", "error_count_reducer.py","error_count.txt"),
    ("Output 3: Top 10 Slow Endpoints (response_time > 800 ms)", "slow_endpoint_mapper.py", "slow_endpoint_reducer.py","slow_endpoint.txt"),
]


def ensure_output_dir():
    
    OUTPUT_DIR.mkdir(exist_ok=True)


def run_mapreduce(mapper_script, reducer_script, input_file):
    """
    模拟 MapReduce pipeline:
    cat input | mapper | sort | reducer
    返回 reducer 的标准输出字符串
    """
    # 1. 读取 CSV 文件并跳过表头
    with open(input_file, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    data_lines = lines[1:] if lines else []
    
    # 2. 启动 mapper 子进程
    mapper_proc = subprocess.Popen(
        [sys.executable, str(MAPREDUCE_DIR / mapper_script)],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        text=True,
        bufsize=1
    )
    mapper_stdout, _ = mapper_proc.communicate(input=''.join(data_lines))
    
    # 3. 排序（模拟 shuffle）
    sorted_lines = sorted(mapper_stdout.splitlines(keepends=True))
    
    # 4. 启动 reducer 子进程
    reducer_proc = subprocess.Popen(
        [sys.executable, str(MAPREDUCE_DIR / reducer_script)],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        text=True
    )
    reducer_stdout, _ = reducer_proc.communicate(input=''.join(sorted_lines))
    
    return reducer_stdout.strip()


def main():
    # 检查数据文件是否存在
    if not DATA_FILE.exists():
        error_msg = f"错误：找不到数据文件 {DATA_FILE}\n请确保 data 目录下存在 Comp3041J MiniProject 2 Dataset.csv"
        print(error_msg)
        return
    
    # 创建 output 文件夹
    ensure_output_dir()
    
    # 依次运行三个作业，只保存文件，不打印到控制台
    for title, mapper, reducer, out_file in JOBS:
        try:
            result = run_mapreduce(mapper, reducer, DATA_FILE)
            output_path = OUTPUT_DIR / out_file
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(result if result else "(无输出)")
        except Exception as e:
            # 发生异常时写入错误信息到对应文件
            output_path = OUTPUT_DIR / out_file
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(f"运行出错：{e}")
    
    # 可选：在控制台输出一个简单的完成提示（如果你完全不希望有任何输出，可以删除下面这行）
    # print("所有结果已保存到 output 文件夹。")


if __name__ == "__main__":
    main()