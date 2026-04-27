"""
연구실 GPU 클러스터 샘플 데이터 생성
Slurm + DCGM 형태
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

np.random.seed(42)
random.seed(42)

# 연구실 설정
USERS = [
    ('prof_kim',    'PI',         0.15),  # 교수 (적게 씀)
    ('phd_lee',     'PhD',        0.20),  # 박사 (많이 씀)
    ('phd_park',    'PhD',        0.18),
    ('phd_choi',    'PhD',        0.15),
    ('ms_jung',     'Masters',    0.10),
    ('ms_han',      'Masters',    0.08),
    ('undergrad_oh','Undergrad',  0.05),
    ('undergrad_lim','Undergrad', 0.04),
    ('visitor_wang','Visitor',    0.03),
    ('visitor_chen','Visitor',    0.02),
]

GPUS = [
    ('gpu-a100-01', 'A100-80GB', 400),
    ('gpu-a100-02', 'A100-80GB', 400),
    ('gpu-a100-03', 'A100-80GB', 400),
    ('gpu-a100-04', 'A100-80GB', 400),
    ('gpu-v100-01', 'V100-32GB', 300),
    ('gpu-v100-02', 'V100-32GB', 300),
    ('gpu-v100-03', 'V100-32GB', 300),
    ('gpu-v100-04', 'V100-32GB', 300),
    ('gpu-rtx-01',  'RTX3090',   350),
    ('gpu-rtx-02',  'RTX3090',   350),
    ('gpu-rtx-03',  'RTX3090',   350),
    ('gpu-rtx-04',  'RTX3090',   350),
]

JOB_TYPES = [
    ('llm_training',     85, 0.30),  # (이름, avg_util, 비율)
    ('cv_training',      75, 0.25),
    ('nlp_finetuning',   60, 0.20),
    ('hyperparameter',   40, 0.10),  # 낭비 많음
    ('inference_test',   15, 0.08),  # 낭비 많음
    ('interactive',      10, 0.07),  # 가장 낭비
]

rows = []
job_rows = []
start_date = datetime(2024, 1, 1)
job_id = 10000

for day in range(60):
    date = start_date + timedelta(days=day)
    is_weekend = date.weekday() >= 5

    for hour in range(24):
        timestamp = date + timedelta(hours=hour)

        # 시간대별 활동 패턴
        if is_weekend:
            activity = 0.3 if 10 <= hour <= 18 else 0.1
        else:
            if 9 <= hour <= 18:
                activity = 0.85
            elif 19 <= hour <= 22:
                activity = 0.60
            elif 23 <= hour or hour <= 7:
                activity = 0.20  # 야간 낭비
            else:
                activity = 0.45

        for gpu_id, gpu_model, tdp in GPUS:
            # GPU별 사용 패턴
            if 'a100' in gpu_id:
                gpu_activity = activity * 1.1
            elif 'v100' in gpu_id:
                gpu_activity = activity * 0.9
            else:
                gpu_activity = activity * 0.7

            gpu_activity = min(gpu_activity, 1.0)

            # 현재 user/job 결정
            if random.random() < gpu_activity:
                user_idx = np.random.choice(
                    len(USERS),
                    p=[u[2] for u in USERS]
                )
                user, role, _ = USERS[user_idx]
                job_type_idx = np.random.choice(
                    len(JOB_TYPES),
                    p=[j[2] for j in JOB_TYPES]
                )
                job_name, avg_util, _ = JOB_TYPES[job_type_idx]
                gpu_util = max(0, min(100,
                    avg_util + np.random.normal(0, 10)
                ))
                mem_used = gpu_util * 0.7 + np.random.normal(0, 5)
                mem_used = max(0, min(100, mem_used))
                power = tdp * (0.2 + gpu_util/100 * 0.8)
                current_job = job_id
            else:
                user = 'idle'
                role = 'none'
                job_name = 'idle'
                gpu_util = max(0, np.random.normal(3, 2))
                mem_used = max(0, np.random.normal(5, 2))
                power = tdp * 0.15
                current_job = None

            temp = 30 + gpu_util * 0.7 + np.random.normal(0, 3)

            rows.append({
                'timestamp':  timestamp,
                'date':       date.date(),
                'hour':       hour,
                'day_of_week': date.weekday(),
                'is_weekend': is_weekend,
                'gpu_id':     gpu_id,
                'gpu_model':  gpu_model,
                'tdp_watts':  tdp,
                'user':       user,
                'user_role':  role,
                'job_id':     current_job,
                'job_type':   job_name,
                'gpu_util':   round(gpu_util, 1),
                'mem_util':   round(mem_used, 1),
                'power_draw': round(power, 1),
                'temperature': round(temp, 1),
                'node':       f'node-{gpu_id.split("-")[1]}',
                'partition':  'gpu-a100' if 'a100' in gpu_id else
                              'gpu-v100' if 'v100' in gpu_id else 'gpu-rtx',
            })

# Slurm job 로그 생성
job_id = 10000
for day in range(60):
    date = start_date + timedelta(days=day)
    is_weekend = date.weekday() >= 5
    n_jobs = 5 if is_weekend else 20

    for _ in range(n_jobs):
        user_idx = np.random.choice(len(USERS), p=[u[2] for u in USERS])
        user, role, _ = USERS[user_idx]
        job_type_idx = np.random.choice(len(JOB_TYPES), p=[j[2] for j in JOB_TYPES])
        job_name, avg_util, _ = JOB_TYPES[job_type_idx]

        submit_hour = random.randint(0, 23)
        wait_time = random.randint(5, 240)  # 분
        run_time = random.randint(30, 480)  # 분

        submit_time = date + timedelta(hours=submit_hour)
        start_time  = submit_time + timedelta(minutes=wait_time)
        end_time    = start_time + timedelta(minutes=run_time)

        gpu_count = random.choice([1, 1, 1, 2, 4])
        gpu_util  = max(0, min(100, avg_util + np.random.normal(0, 15)))

        job_rows.append({
            'job_id':       job_id,
            'user':         user,
            'user_role':    role,
            'job_name':     job_name,
            'partition':    'gpu-a100' if gpu_count >= 2 else random.choice(['gpu-v100','gpu-rtx','gpu-a100']),
            'submit_time':  submit_time,
            'start_time':   start_time,
            'end_time':     end_time,
            'wait_minutes': wait_time,
            'run_minutes':  run_time,
            'gpu_count':    gpu_count,
            'avg_gpu_util': round(gpu_util, 1),
            'exit_code':    0 if random.random() > 0.1 else 1,
            'date':         date.date(),
        })
        job_id += 1

df_metrics = pd.DataFrame(rows)
df_jobs    = pd.DataFrame(job_rows)

df_metrics.to_csv('lab_gpu_metrics.csv', index=False)
df_jobs.to_csv('lab_slurm_jobs.csv', index=False)

print(f'GPU metrics: {len(df_metrics):,} rows')
print(f'Slurm jobs:  {len(df_jobs):,} rows')
print(f'Users:       {df_metrics["user"].nunique()}')
print(f'GPUs:        {df_metrics["gpu_id"].nunique()}')
print(f'Avg util:    {df_metrics["gpu_util"].mean():.1f}%')
print(f'Idle rows:   {(df_metrics["gpu_util"]<15).sum()/len(df_metrics)*100:.1f}%')
