#!/usr/bin/env python3

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