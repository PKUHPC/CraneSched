import json
import random
from datetime import datetime, timedelta

start_time = datetime(2025, 1, 1, 0, 0, 0)
end_time = datetime(2025, 3, 3, 0, 0, 0)

records = []
task_id = 1
current_time = start_time

num_accounts = 10  # 账户和用户数量

while current_time <= end_time:
    time_start = int(current_time.timestamp())
    for acc_num in range(1, num_accounts + 1):  # 1~100
        account_name = f"account_{acc_num}"
        username = f"user_{acc_num}"
        for _ in range(100):  # 每个账户/用户每小时100条
            time_end = time_start + random.randint(10, 100000)
            cpus_alloc = float(random.randint(1, 10))
            nodes_alloc = random.randint(1, 50)
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
