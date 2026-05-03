"""
power.py
────────
역할: 전력 낭비 감지
      1) 오버프로비저닝 — 피크타임 포함해도 낮은 util
      2) 전력 효율 점수
      3) 과열 감지

알고리즘:
  - 피크타임(9-18시) 데이터만 따로 분리해서 오버프로비저닝 판단
  - PES = util / (power_draw / power_limit)
"""

import pandas as pd
from .anomaly import ema

OVERPROVISION_PEAK_MAX  = 30.0   # 피크타임에도 이 이하면 오버프로비저닝
OVERPROVISION_MIN_HOURS = 3      # 최소 N시간 피크 데이터 필요
THERMAL_WARNING_C       = 83
THERMAL_CRITICAL_C      = 90
PES_LOW_THRESHOLD       = 0.3


def detect(df: pd.DataFrame) -> list:
    alerts = []
    if df.empty:
        return alerts

    # 피크타임 데이터 분리
    df = df.copy()
    df['hour'] = pd.to_datetime(df['timestamp']).dt.hour
    peak_df = df[df['hour'].between(9, 18)]

    latest_ts = df['timestamp'].max()
    latest    = df[df['timestamp'] == latest_ts]

    for gpu_idx in df['gpu_index'].unique():
        gpu_df      = df[df['gpu_index'] == gpu_idx].copy()
        gpu_peak    = peak_df[peak_df['gpu_index'] == gpu_idx]
        gpu_latest  = latest[latest['gpu_index'] == gpu_idx]

        if gpu_latest.empty:
            continue

        row = gpu_latest.iloc[-1]

        # ── 1. 과열 감지 ──
        temp = row.get('temperature', 0)
        if temp >= THERMAL_WARNING_C:
            severity = 'critical' if temp >= THERMAL_CRITICAL_C else 'high'
            alerts.append({
                'type'      : 'THERMAL_WARNING',
                'severity'  : severity,
                'gpu_index' : int(gpu_idx),
                'gpu_name'  : row['gpu_name'],
                'temp_c'    : round(float(temp), 1),
                'message'   : (
                    f"GPU-{gpu_idx} overheating: {temp:.0f}°C "
                    f"({'critical' if temp >= THERMAL_CRITICAL_C else 'warning'})"
                ),
                'command'   : (
                    f"# 팬 속도 확인:\n"
                    f"nvidia-smi -i {gpu_idx} --query-gpu=fan.speed --format=csv\n"
                    f"# 전력 제한으로 온도 낮추기:\n"
                    f"nvidia-smi -i {gpu_idx} -pl {int(row.get('power_limit', 300) * 0.8)}"
                ),
                'rollback'  : f"nvidia-smi -i {gpu_idx} -pl {int(row.get('power_limit', 300))}",
                'auto_safe' : False,
            })

        # ── 2. 전력 효율 점수 ──
        power_draw  = row.get('power_draw', 0)
        power_limit = row.get('power_limit', 1)
        util        = row.get('gpu_util', 0)

        if power_limit > 0 and power_draw > 10:
            power_ratio = power_draw / power_limit
            pes = (util / 100) / power_ratio if power_ratio > 0 else 0

            if pes < PES_LOW_THRESHOLD and util < 20:
                monthly_waste = power_draw * 24 * 30 / 1000 * 0.12
                alerts.append({
                    'type'             : 'LOW_POWER_EFFICIENCY',
                    'severity'         : 'medium',
                    'gpu_index'        : int(gpu_idx),
                    'gpu_name'         : row['gpu_name'],
                    'pes'              : round(pes, 3),
                    'util_pct'         : round(float(util), 1),
                    'power_draw_w'     : round(float(power_draw), 1),
                    'monthly_waste_usd': round(monthly_waste, 2),
                    'message'          : (
                        f"GPU-{gpu_idx} low efficiency (PES={pes:.2f}): "
                        f"{util:.0f}% util drawing {power_draw:.0f}W "
                        f"(${monthly_waste:.0f}/mo wasted)"
                    ),
                    'command'          : f"nvidia-smi -i {gpu_idx} -pl {max(75, int(power_draw * 0.6))}",
                    'rollback'         : f"nvidia-smi -i {gpu_idx} -pl {int(power_limit)}",
                    'auto_safe'        : True,
                })

        # ── 3. 오버프로비저닝 — 피크타임 기준 ──
        if len(gpu_peak) >= OVERPROVISION_MIN_HOURS * 12:  # 5분 수집 기준
            peak_util_avg = gpu_peak['gpu_util'].mean()

            if peak_util_avg < OVERPROVISION_PEAK_MAX:
                power_limit_val = row.get('power_limit', 400)
                monthly_cost    = power_limit_val * 24 * 30 / 1000 * 0.12
                potential_save  = monthly_cost * 0.6

                alerts.append({
                    'type'              : 'OVERPROVISIONED',
                    'severity'          : 'medium',
                    'gpu_index'         : int(gpu_idx),
                    'gpu_name'          : row['gpu_name'],
                    'peak_util'         : round(float(peak_util_avg), 1),
                    'power_limit_w'     : round(float(power_limit_val), 0),
                    'potential_save_usd': round(potential_save, 2),
                    'message'           : (
                        f"GPU-{gpu_idx} overprovisioned: "
                        f"{peak_util_avg:.0f}% avg during peak hours (9-18h) "
                        f"(could save ${potential_save:.0f}/mo)"
                    ),
                    'command'           : f"nvidia-smi -i {gpu_idx} -pl {int(power_limit_val * 0.5)}",
                    'rollback'          : f"nvidia-smi -i {gpu_idx} -pl {int(power_limit_val)}",
                    'auto_safe'         : True,
                })

    return alerts
