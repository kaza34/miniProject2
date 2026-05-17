#!/usr/bin/env python3

import sys

first_line = True

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    if first_line:
        first_line = False
        continue
    parts = line.split(',')
    if len(parts) < 8:
        continue
    try:
        rt = int(parts[7])   # response_time_ms
    except ValueError:
        continue
    if rt > 800:
        service = parts[3]
        endpoint = parts[4]
        key = f"{service},{endpoint}"
        print(f"{key}\t1")