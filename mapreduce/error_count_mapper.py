import sys
import csv

reader = csv.reader(sys.stdin)
header = next(reader, None)
if header is None:
    sys.exit(0)

for row in reader:
    if len(row) < 7:
        continue
    try:
        status = int(row[6].strip())
        if status >= 500:
            service = row[3].strip()
            sys.stdout.write(f"{service}\t1\n")
    except (ValueError, IndexError):
        continue