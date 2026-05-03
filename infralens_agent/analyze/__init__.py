"""
analyze/__init__.py
───────────────────
역할: 모든 감지 모듈을 하나로 통합
"""

import pandas as pd
from . import idle, memory, zombie, balance, power

SEVERITY_ORDER = {'critical': 0, 'high': 1, 'medium': 2, 'low': 3}

def run_all(df: pd.DataFrame) -> dict:
    if df.empty:
        return {'alerts': [], 'summary': {}, 'recommendations': []}

    all_alerts = []
    all_alerts += idle.detect(df)
    all_alerts += memory.detect(df)
    all_alerts += zombie.detect(df)
    all_alerts += balance.detect(df)
    all_alerts += power.detect(df)

    # severity 순 정렬
    all_alerts.sort(key=lambda a: SEVERITY_ORDER.get(a['severity'], 99))

    latest_ts = df['timestamp'].max()
    latest    = df[df['timestamp'] == latest_ts]

    summary = {
        'n_gpus'       : int(df['gpu_index'].nunique()),
        'overall_util' : round(float(df['gpu_util'].mean()), 1),
        'idle_pct'     : round(float((df['gpu_util'] < 15).mean() * 100), 1),
        'total_power_w': round(float(latest['power_draw'].sum()), 1),
        'n_alerts'     : len(all_alerts),
        'n_critical'   : sum(1 for a in all_alerts if a['severity'] == 'critical'),
        'n_high'       : sum(1 for a in all_alerts if a['severity'] == 'high'),
        'n_medium'     : sum(1 for a in all_alerts if a['severity'] == 'medium'),
    }

    # 권장사항: auto_safe 먼저, severity 순, 절감액 큰 것 먼저
    recs = [a for a in all_alerts if 'command' in a]
    recs.sort(key=lambda a: (
        SEVERITY_ORDER.get(a['severity'], 99),
        0 if a.get('auto_safe') else 1,
        -a.get('savings_monthly_usd', a.get('monthly_waste_usd', a.get('potential_save_usd', 0)))
    ))

    return {
        'alerts'         : all_alerts,
        'summary'        : summary,
        'recommendations': recs,
    }
