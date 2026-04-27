"""
analyze.py
──────────
역할: 로컬 DB에서 데이터 읽어서 분석
      외부 전송 없음
"""
import sqlite3
import pandas as pd
import json
from datetime import datetime, timedelta
from pathlib import Path
import yaml

CONFIG_PATH = Path(__file__).parent / 'config.yaml'

def load_config():
    with open(CONFIG_PATH) as f:
        return yaml.safe_load(f)


def load_recent_data(db_path: str, hours: int = 24) -> pd.DataFrame:
    """최근 N시간 데이터 로드"""
    conn = sqlite3.connect(db_path)
    cutoff = (datetime.now() - timedelta(hours=hours)).isoformat()

    df = pd.read_sql_query(f'''
        SELECT * FROM gpu_metrics
        WHERE timestamp > '{cutoff}'
        ORDER BY timestamp
    ''', conn)
    conn.close()
    return df


def analyze_current_state(df: pd.DataFrame) -> dict:
    """현재 상태 분석"""
    if df.empty:
        return {}

    results = {}

    # 전체 사용률
    results['overall_util']  = round(df['gpu_util'].mean(), 1)
    results['idle_pct']      = round((df['gpu_util'] < 15).sum() / len(df) * 100, 1)
    results['n_gpus']        = df['gpu_index'].nunique()

    # idle GPU 목록
    latest_ts = df['timestamp'].max()
    latest = df[df['timestamp'] == latest_ts]
    idle_gpus = latest[latest['gpu_util'] < 15]
    results['currently_idle'] = idle_gpus[['gpu_index','gpu_name','gpu_util','power_draw']].to_dict('records')
    results['n_idle_now']     = len(idle_gpus)

    # 전력
    results['total_power_w'] = round(latest['power_draw'].sum(), 1)
    results['idle_power_w']  = round(idle_gpus['power_draw'].sum(), 1) if len(idle_gpus) > 0 else 0

    # 시간대별 패턴
    if 'timestamp' in df.columns:
        df['hour'] = pd.to_datetime(df['timestamp']).dt.hour
        hourly = df.groupby('hour')['gpu_util'].mean().round(1)
        results['hourly_util'] = hourly.to_dict()
        results['peak_hour']   = int(hourly.idxmax())
        results['worst_hour']  = int(hourly.idxmin())

    # 온도 경고
    hot = latest[latest['temperature'] > 85]
    results['thermal_warning'] = hot[['gpu_index','gpu_name','temperature']].to_dict('records')

    return results


def detect_anomalies(df: pd.DataFrame) -> list:
    """이상 감지 - 알림 대상"""
    alerts = []
    if df.empty:
        return alerts

    latest_ts = df['timestamp'].max()
    latest    = df[df['timestamp'] == latest_ts]

    # 장시간 idle GPU
    for _, row in latest.iterrows():
        if row['gpu_util'] < 5:
            # 얼마나 오래 idle이었는지 확인
            gpu_history = df[df['gpu_index'] == row['gpu_index']].tail(6)
            if (gpu_history['gpu_util'] < 5).all():
                alerts.append({
                    'type':    'IDLE_GPU',
                    'gpu':     f"GPU-{row['gpu_index']} ({row['gpu_name']})",
                    'util':    row['gpu_util'],
                    'power':   row['power_draw'],
                    'message': f"GPU-{row['gpu_index']} has been idle for 6+ hours at {row['gpu_util']:.1f}% util",
                    'severity': 'high',
                })

    # 온도 경고
    for _, row in latest.iterrows():
        if row['temperature'] > 85:
            alerts.append({
                'type':    'THERMAL_WARNING',
                'gpu':     f"GPU-{row['gpu_index']}",
                'temp':    row['temperature'],
                'message': f"GPU-{row['gpu_index']} temperature is {row['temperature']:.0f}°C",
                'severity': 'critical',
            })

    return alerts


def generate_recommendations(state: dict, mode: str = 'business') -> list:
    """분석 결과 → 실행 가능한 권장사항"""
    recs = []

    # Idle GPU 전력 제한
    idle_gpus = state.get('currently_idle', [])
    if len(idle_gpus) >= 2:
        commands = '\n'.join([
            f"  nvidia-smi -i {g['gpu_index']} -pl 75  # {g['gpu_name']} ({g['gpu_util']:.1f}% util)"
            for g in idle_gpus[:8]
        ])
        recs.append({
            'priority': 1,
            'category': 'Idle Waste',
            'title':    f'{len(idle_gpus)} GPUs currently idle — power limiting saves ~70% idle power',
            'command':  commands,
            'rollback': '\n'.join([f"  nvidia-smi -i {g['gpu_index']} -pl 400" for g in idle_gpus[:8]]),
            'risk':     'Low',
            'auto_safe': True,  # 자동 실행 가능 여부
        })

    return recs


def run_analysis():
    config  = load_config()
    db_path = Path(__file__).parent / config['storage']['db_path']

    if not db_path.exists():
        print('No data yet — run collect.py first')
        return

    print('InfraLens Agent — Analysis')
    print()

    df    = load_recent_data(str(db_path), hours=24)
    state = analyze_current_state(df)
    alerts = detect_anomalies(df)
    recs   = generate_recommendations(state, config['lab']['mode'])

    print(f'GPUs:          {state.get("n_gpus", 0)}')
    print(f'Overall util:  {state.get("overall_util", 0)}%')
    print(f'Idle now:      {state.get("n_idle_now", 0)} GPUs')
    print(f'Total power:   {state.get("total_power_w", 0)}W')
    print(f'Idle power:    {state.get("idle_power_w", 0)}W')
    print()

    if alerts:
        print(f'⚠️  {len(alerts)} alerts:')
        for a in alerts:
            print(f'  [{a["severity"].upper()}] {a["message"]}')
    else:
        print('✅ No alerts')

    print()
    if recs:
        print(f'💡 {len(recs)} recommendations:')
        for r in recs:
            print(f'  #{r["priority"]} {r["title"]}')
    else:
        print('✅ No recommendations')

    return {'state': state, 'alerts': alerts, 'recommendations': recs}


if __name__ == '__main__':
    run_analysis()
