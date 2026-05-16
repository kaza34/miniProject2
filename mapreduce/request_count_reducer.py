#!/usr/bin/env python3
# import sys
#
# current_service = None
# current_count = 0
#
# for line in sys.stdin:
#     service, count = line.strip().split("\t")
#     count = int(count)
#
#     if current_service == service:
#         current_count += count
#     else:
#         if current_service:
#             print(f"{current_service}\t{current_count}")
#
#         current_service = service
#         current_count = count
#
# if current_service:
#     print(f"{current_service}\t{current_count}")
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