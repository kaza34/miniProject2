#!/usr/bin/env python3
# import sys
#
# counts = {}
#
# for line in sys.stdin:
#     key, count = line.strip().split("\t")
#     count = int(count)
#
#     counts[key] = counts.get(key, 0) + count
#
# top10 = sorted(counts.items(), key=lambda x: x[1], reverse=True)[:10]
#
# for key, count in top10:
#     print(f"{key}\t{count}")
import sys
from collections import defaultdict

counter = defaultdict(int)

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    key, count = line.split('\t', 1)
    try:
        count = int(count)
    except ValueError:
        continue
    counter[key] += count

# 排序取 top 10
sorted_items = sorted(counter.items(), key=lambda x: x[1], reverse=True)[:10]

for key, total in sorted_items:
    print(f"{key}\t{total}")