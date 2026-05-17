#!/usr/bin/env python3
import sys

current_service = None
total = 0

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    service, count = line.split('\t', 1)
    try:
        count = int(count)
    except ValueError:
        continue
    if service == current_service:
        total += count
    else:
        if current_service is not None:
            print(f"{current_service}\t{total}")
        current_service = service
        total = count

if current_service is not None:
    print(f"{current_service}\t{total}")