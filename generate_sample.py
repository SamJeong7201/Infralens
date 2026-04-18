import pandas as pd
import numpy as np

np.random.seed(42)

dates = pd.date_range('2024-01-01', periods=30*24, freq='h')
gpu_ids = [f'gpu-{i:02d}' for i in range(1, 9)]

rows = []
for ts in dates:
    hour = ts.hour
    for gpu_id in gpu_ids:
        if 1 <= hour <= 5:
            util = np.random.uniform(0, 12)
        elif 9 <= hour <= 18:
            util = np.random.uniform(55, 95)
        else:
            util = np.random.uniform(15, 45)

        power = 120 + (util / 100) * 580 + np.random.normal(0, 10)

        if 14 <= hour <= 18:
            cost_per_hr = 4.10
        elif 1 <= hour <= 6:
            cost_per_hr = 2.10
        else:
            cost_per_hr = 3.20

        if util < 10:
            job_type = 'idle'
        elif util > 70:
            job_type = 'training'
        else:
            job_type = 'inference'

        rows.append({
            'timestamp': ts,
            'gpu_id': gpu_id,
            'gpu_util_pct': round(util, 1),
            'power_watt': round(power, 1),
            'job_type': job_type,
            'cost_per_hr': cost_per_hr,
        })

df = pd.DataFrame(rows)
df.to_csv('gpu_metrics_30d.csv', index=False)

print(f"생성 완료: {len(df):,}행")
print(f"기간: {df.timestamp.min()} ~ {df.timestamp.max()}")
print()
print("=== 시간대별 평균 GPU 사용률 ===")
df['hour'] = df.timestamp.dt.hour
hourly = df.groupby('hour')['gpu_util_pct'].mean()
for hour, util in hourly.items():
    bar = '█' * int(util / 5)
    flag = ' ◀ 낭비 구간!' if util < 15 else ''
    print(f"  {hour:02d}시: {bar} {util:.1f}%{flag}")
