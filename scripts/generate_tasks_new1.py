import json
import random
from datetime import datetime, timedelta

start_time = datetime(2024, 12, 2, 23, 0, 0)
end_time = datetime(2025, 11, 26, 0, 0, 0)


records = []
task_id = 1
current_time = start_time
num_per_hour = 5
account_range = 2
max_total = 10  # 只生成10000条

while current_time <= end_time and len(records) < max_total:
    time_start = int(current_time.timestamp())
    for _ in range(num_per_hour):
        if len(records) >= max_total:
            break
        acc_num = random.randint(1, account_range)
        account_name = f"account_{acc_num}"
        username = f"user_{acc_num}"
        time_end = time_start + 10
        cpus_alloc = 2
        nodes_alloc = 1.0
        records.append({
            "task_id": task_id,
            "account": account_name,
            "username": username,
            "cpus_alloc": cpus_alloc,
            "nodes_alloc": nodes_alloc,
            "time_start": time_start,
            "time_end": time_end
        })
        task_id += 1
    current_time += timedelta(hours=1)

with open("tasks_bulk.json", "w") as f:
    json.dump(records, f, indent=2)

print(f"生成数据条数: {len(records)}")

