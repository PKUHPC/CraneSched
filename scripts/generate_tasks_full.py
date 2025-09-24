import json
import random
from datetime import datetime, timedelta

start_time = datetime(2024, 9, 1, 0, 0, 0)
end_time = datetime(2025, 11, 26, 0, 0, 0)

records = []
task_id = 1
current_time = start_time
num_per_hour =1000
account_range = 10000
max_total = 10000  # 只生成10000条

while current_time <= end_time and len(records) < max_total:
    time_start = int(current_time.timestamp())
    for _ in range(num_per_hour):
        if len(records) >= max_total:
            break
        acc_num = random.randint(1, account_range)
        account_name = f"account_{acc_num}"
        username = f"user_{acc_num}"
        time_end = time_start + random.randint(10, 100000)
        cpus_alloc = float(random.randint(1, 100))
        nodes_alloc = random.randint(1, 100)

        # 固定字段补充
        record = {
            "task_id": task_id,
            "task_db_id": task_id,
            "mod_time": 1758681810,
            "deleted": False,
            "account": account_name,
            "cpus_req": cpus_alloc,
            "mem_req": 3000000000,
            "task_name": "",
            "env": "{ }",
            "id_user": 0,
            "id_group": 0,
            "nodelist": "f16918890416",
            "nodes_alloc": nodes_alloc,
            "node_inx": 0,
            "partition_name": "CPU",
            "priority": 0,
            "time_eligible": 0,
            "time_start": time_start,
            "time_end": time_end,
            "time_suspended": 0,
            "script": "#!/bin/bash\nsleep 10\nhostname\n\necho \"Job ID: $CRANE_JOB_ID\"",
            "state": 2,
            "timelimit": 70,
            "time_submit": time_start - random.randint(10, 100),  # 随机比start早
            "work_dir": "/Workspace/CraneSched-FrontEnd/out",
            "submit_line": "cbatch testjob.sh",
            "exit_code": 0,
            "username": username,
            "qos": "UNLIMITED",
            "get_user_env": False,
            "type": 1,
            "extra_attr": "",
            "reservation": "",
            "exclusive": False,
            "cpus_alloc": cpus_alloc,
            "mem_alloc": 3000000000,
            "device_map": {},
            "container": ""
        }

        records.append(record)
        task_id += 1
    current_time += timedelta(hours=1)

with open("tasks_bulk.json", "w") as f:
    json.dump(records, f, indent=2)

print(f"生成数据条数: {len(records)}")

