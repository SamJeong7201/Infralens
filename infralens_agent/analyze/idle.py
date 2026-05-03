"""
idle.py
───────
역할: Idle GPU 감지
      EMA로 노이즈 제거 + 피크타임 고려

알고리즘: EMA + 시간대별 기대 사용률
  - 업무시간(9-18시)엔 기준 높임
  - 야간/주말엔 기준 낮춤
  - 순간 spike는 무시하고 지속적 idle만 감지
"""

import pandas as pd
from datetime import datetime
from .anomaly import ema

IDLE_EMA_THRESHOLD   = 10.0
IDLE_MIN_PERIODS     = 6      # 최소 6개 포인트 (5분 수집 기준 30분)
IDLE_POWER_MIN_WATTS = 50


def _expected_util(hour: int) -> float:
    """시간대별 기대 사용률 — 이 이하면 idle 의심"""
    if 9 <= hour <= 18:
        return 20.0   # 업무시간: 20% 이하면 idle
    elif 19 <= hour <= 22:
        return 12.0   # 저녁
    else:
        return 5.0    # 야간/새벽


def detect(df: pd.DataFrame) -> list:
    alerts = []
    if df.empty:
        return alerts

    current_hour = datetime.now().hour
    threshold = _expected_util(current_hour)

    for gpu_idx in df['gpu_index'].unique():
        gpu_df = df[df['gpu_index'] == gpu_idx].copy()
        gpu_df = gpu_df.sort_values('timestamp')

        if len(gpu_df) < IDLE_MIN_PERIODS:
            continue

        gpu_df['util_ema'] = ema(gpu_df['gpu_util'], span=6)
        latest = gpu_df.iloc[-1]

        if latest['util_ema'] > threshold:
            continue

        # 얼마나 오래 idle이었는지
        idle_rows = gpu_df[gpu_df['util_ema'] < threshold]
        idle_periods = len(idle_rows)
        idle_hours = round(idle_periods * 5 / 60, 1)  # 5분 수집 기준

        if idle_periods < IDLE_MIN_PERIODS:
            continue

        power = latest.get('power_draw', 0)
        if power < IDLE_POWER_MIN_WATTS:
            continue

        savings_monthly = power * 0.70 * 24 * 30 / 1000 * 0.12

        # 업무시간 idle은 severity 높임
        severity = 'high' if 9 <= current_hour <= 18 else 'medium'

        alerts.append({
            'type'               : 'IDLE_GPU',
            'severity'           : severity,
            'gpu_index'          : int(gpu_idx),
            'gpu_name'           : latest['gpu_name'],
            'util_now'           : round(float(latest['gpu_util']), 1),
            'util_ema'           : round(float(latest['util_ema']), 1),
            'power_w'            : round(float(power), 1),
            'idle_hours'         : idle_hours,
            'time_context'       : 'business_hours' if 9 <= current_hour <= 18 else 'off_hours',
            'savings_monthly_usd': round(savings_monthly, 2),
            'message'            : (
                f"GPU-{gpu_idx} ({latest['gpu_name']}) idle {idle_hours}h "
                f"at {latest['util_ema']:.1f}% (expected >{threshold:.0f}% at {current_hour}:00) "
                f"— wasting {power:.0f}W (${savings_monthly:.0f}/mo)"
            ),
            'command'            : f"nvidia-smi -i {gpu_idx} -pl 75",
            'rollback'           : f"nvidia-smi -i {gpu_idx} -pl {int(latest.get('power_limit', 400))}",
            'auto_safe'          : True,
        })

    return alerts
