import sys

current_service = None
total = 0

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    parts = line.split('\t')
    if len(parts) < 2:
        continue
    service = parts[0]
    try:
        count = int(parts[1])
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
