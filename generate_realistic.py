import pandas as pd
import numpy as np
from datetime import datetime, timedelta

np.random.seed(42)

start = datetime(2024, 1, 1)
records = []

gpu_configs = [
    # gpu_id,        model,  base_util, peak_start, peak_end, cost_hr
    ('gpu-a100-01', 'A100', 82, 9, 18, 4.10),   # 핵심 학습 GPU
    ('gpu-a100-02', 'A100', 78, 9, 18, 4.10),   # 핵심 학습 GPU
    ('gpu-a100-03', 'A100', 71, 10, 19, 4.10),  # 핵심 학습 GPU
    ('gpu-a100-04', 'A100', 75, 8, 17, 4.10),   # 핵심 학습 GPU
    ('gpu-a100-05', 'A100', 42, 9, 18, 4.10),   # 가끔 사용
    ('gpu-a100-06', 'A100', 28, 9, 18, 4.10),   # 거의 idle
    ('gpu-a100-07', 'A100', 12, 9, 18, 4.10),   # idle (낭비)
    ('gpu-a100-08', 'A100', 8,  9, 18, 4.10),   # idle (낭비)
    ('gpu-v100-01', 'V100', 68, 9, 18, 2.80),   # inference
    ('gpu-v100-02', 'V100', 62, 9, 18, 2.80),   # inference
    ('gpu-v100-03', 'V100', 55, 10, 20, 2.80),  # inference
    ('gpu-v100-04', 'V100', 45, 9, 18, 2.80),   # 가끔 사용
    ('gpu-v100-05', 'V100', 18, 9, 18, 2.80),   # 거의 idle
    ('gpu-v100-06', 'V100', 12, 9, 18, 2.80),   # idle (낭비)
    ('gpu-v100-07', 'V100', 7,  9, 18, 2.80),   # idle (낭비)
    ('gpu-v100-08', 'V100', 5,  9, 18, 2.80),   # idle (낭비)
]

for day in range(60):
    current_date = start + timedelta(days=day)
    is_weekend = current_date.weekday() >= 5

    for hour in range(24):
        for gpu_id, model, base_util, peak_s, peak_e, cost in gpu_configs:

            # 시간대 팩터
            in_peak = peak_s <= hour < peak_e

            if is_weekend:
                if in_peak:
                    time_factor = 0.12
                else:
                    time_factor = 0.06
            else:
                if in_peak:
                    time_factor = 1.0
                elif 6 <= hour < peak_s or peak_e <= hour < 22:
                    time_factor = 0.35
                else:
                    time_factor = 0.08

            # 실제 사용률
            util = base_util * time_factor
            util += np.random.normal(0, max(3, base_util * 0.08))
            util = max(2, min(98, util))

            # 가끔 스파이크
            if np.random.random() < 0.03:
                util = min(98, util + np.random.uniform(20, 40))

            # 가끔 maintenance (util=0)
            if np.random.random() < 0.005:
                util = 0.0
                job_type = 'maintenance'
            elif util > 60:
                job_type = 'training'
            elif util > 25:
                job_type = 'inference'
            else:
                job_type = 'idle'

            # 전력 (W)
            tdp = 400 if model == 'A100' else 300
            idle_power = tdp * 0.15
            power_w = idle_power + (tdp - idle_power) * (util / 100)
            power_w += np.random.normal(0, 8)
            power_w = max(idle_power * 0.8, min(tdp * 1.02, power_w))

            # 메모리
            memory_util = util * 0.82 + np.random.normal(0, 6)
            memory_util = max(5, min(95, memory_util))

            # 온도
            temp_c = 32 + util * 0.52 + np.random.normal(0, 2.5)
            temp_c = max(28, min(88, temp_c))

            # TOU 요금
            if 8 <= hour < 22 and not is_weekend:
                rate = cost
            else:
                rate = cost * 0.51  # off-peak 49% 저렴

            records.append({
                'timestamp':         current_date + timedelta(hours=hour),
                'gpu_id':            gpu_id,
                'gpu_model':         model,
                'gpu_util_pct':      round(util, 1),
                'memory_util_pct':   round(memory_util, 1),
                'power_watt':        round(power_w, 1),
                'temp_celsius':      round(temp_c, 1),
                'cost_per_hr':       rate,
                'job_type':          job_type,
                'datacenter_region': 'us-east-1',
            })

df = pd.DataFrame(records)
df.to_csv('realistic_gpu_data.csv', index=False)

print(f"Generated: {len(df):,} rows")
print(f"GPUs: {df['gpu_id'].nunique()}")
print(f"Days: {df['timestamp'].dt.date.nunique()}")
print(f"\nAvg util by GPU:")
print(df.groupby('gpu_id')['gpu_util_pct'].mean().round(1).to_string())
print(f"\nAvg cost/hr: ${df['cost_per_hr'].mean():.3f}")
print(f"Avg power: {df['power_watt'].mean():.1f}W")
total_cost = df['power_watt'].sum() / 1000 * df['cost_per_hr'].mean()
print(f"Est monthly cost: ${total_cost * 30 / 60:,.0f}")
