#!/usr/bin/env python3
# import sys
#
# for line in sys.stdin:
#     line = line.strip()
#
#     if not line:
#         continue
#
#     fields = line.split(",")
#
#     if fields[0] == "timestamp":
#         continue
#
#     service = fields[3]
#
#     print(f"{service}\t1")
#!/usr/bin/env python3
# mapper1.py
# 功能：读取 CSV 每行，输出 (service_name, 1)

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
    if len(parts) < 4:
        continue
    service = parts[3]   # service_name
    print(f"{service}\t1")