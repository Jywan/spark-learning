import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

OUTPUT_PATH = Path("data/web_logs.csv")
ROW_COUNT = 100_000

PATHS = ["/", "/products", "/products/1", "/products/2", "/products/3", "/cart", "/checkout"]
METHODS = ["GET", "POST"]
STATUS_CODES = [200, 200, 200, 200, 200, 404, 500]

def generate_logs(row_count: int) -> None:
    start_time = datetime(2026, 1, 1, 0, 0, 0)

    with OUTPUT_PATH.open("w", newline="") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=["timestamp", "user_id", "path", "status_code", "response_time_ms"])
        writer.writeheader()

        for i in range(row_count):
            timestamp = start_time + timedelta(seconds=i)
            path = random.choice(PATHS)
            status_code = random.choice(STATUS_CODES)
            response_time_ms = random.randint(5, 300) if status_code == 200 else random.randint(100, 500)

            writer.writerow({
                "timestamp": timestamp.isoformat(timespec="seconds"),
                "user_id": f"u{random.randint(1, 1000):04d}",
                "path": path,
                "status_code": status_code,
                "response_time_ms": response_time_ms,
            })

    print(f"{row_count}행 생성 완료 → {OUTPUT_PATH}")

if __name__ == "__main__":
    generate_logs(ROW_COUNT)
