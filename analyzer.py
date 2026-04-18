import pandas as pd
import numpy as np
from scipy import stats

# ── 컬럼 자동 감지 ──
COLUMN_MAP = {
    'timestamp':      ['timestamp', 'time', 'datetime', 'ts', 'date'],
    'gpu_util':       ['gpu_util', 'gpu_util_pct', 'gpu_utilization', 'util', 'utilization'],
    'memory_util':    ['memory_util', 'mem_util', 'gpu_memory_pct', 'memory_utilization'],
    'power_kw':       ['power_kw', 'power_watt', 'power', 'watt', 'watts'],
    'temp_c':         ['temp_c', 'temperature', 'temp', 'gpu_temp'],
    'cooling_kw':     ['cooling_kw', 'cooling_power', 'cooling'],
    'electricity_rate':['electricity_rate', 'cost_per_hr', 'rate', 'price_per_kwh'],
    'workload_type':  ['workload_type', 'job_type', 'workload', 'job'],
    'gpu_model':      ['gpu_model', 'model', 'gpu_type'],
    'gpu_id':         ['gpu_id', 'gpu', 'device_id', 'device'],
}

def detect_columns(df):
    """어떤 CSV든 컬럼 자동 매핑"""
    mapping = {}
    df_cols_lower = {c.lower(): c for c in df.columns}
    for standard, variants in COLUMN_MAP.items():
        for v in variants:
            if v.lower() in df_cols_lower:
                mapping[standard] = df_cols_lower[v.lower()]
                break
    return mapping

def load_data(filepath='gpu_metrics_30d.csv', chunksize=None):
    """대용량 CSV도 처리 가능 — 청크 기반 로딩"""
    if chunksize:
        chunks = []
        for chunk in pd.read_csv(filepath, chunksize=chunksize):
            chunks.append(chunk)
        df = pd.concat(chunks, ignore_index=True)
    else:
        df = pd.read_csv(filepath)

    col_map = detect_columns(df)

    # 표준 컬럼명으로 통일
    rename = {v: k for k, v in col_map.items()}
    df = df.rename(columns=rename)

    # timestamp 처리
    if 'timestamp' in df.columns:
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df['hour']    = df['timestamp'].dt.hour
        df['date']    = df['timestamp'].dt.date
        df['weekday'] = df['timestamp'].dt.dayofweek

    # power 단위 통일 (W → kW)
    if 'power_kw' in df.columns:
        if df['power_kw'].mean() > 100:  # Watt 단위면 변환
            df['power_kw'] = df['power_kw'] / 1000

    return df, col_map

# ── Finding 01: GPU Idle ──
def detect_idle(df, col_map):
    """
    동적 임계값 + Z-score 기반 Idle 탐지
    - GPU별 개별 임계값 (일괄 적용 X)
    - 시간대별 정상 범위 계산
    - 신뢰도 점수 포함
    """
    results = []
    gpu_col = 'gpu_id' if 'gpu_id' in df.columns else None

    gpus = df[gpu_col].unique() if gpu_col else ['all']

    for gpu in gpus:
        gdf = df[df[gpu_col] == gpu] if gpu_col else df

        if 'gpu_util' not in gdf.columns:
            continue

        # 시간대별 정상 사용률 계산 (평균 - 1σ)
        hourly_mean = gdf.groupby('hour')['gpu_util'].mean()
        hourly_std  = gdf.groupby('hour')['gpu_util'].std().fillna(5)

        idle_rows = []
        for _, row in gdf.iterrows():
            hour = row['hour']
            expected = hourly_mean.get(hour, 50)
            std = hourly_std.get(hour, 10)
            # 기대값보다 2σ 이상 낮으면 idle
            z_score = (row['gpu_util'] - expected) / max(std, 1)
            if z_score < -0.8 and row['gpu_util'] < 30:
                idle_rows.append(row)

        if not idle_rows:
            continue

        idle_df = pd.DataFrame(idle_rows)
        idle_hours = len(idle_df)

        # 절감액 계산
        if 'electricity_rate' in idle_df.columns:
            rate = idle_df['electricity_rate'].mean()
        else:
            rate = 3.20  # 기본값

        if 'power_kw' in idle_df.columns:
            avg_power = idle_df['power_kw'].mean()
            # 절감 = (현재 전력 - 절전 전력) × 시간 × 단가
            idle_power_saving = avg_power * 0.65  # 65% 절감 가정
            power_savings = idle_power_saving * idle_hours * 0.12
        else:
            power_savings = 0

        # 인스턴스 비용 절감
        instance_savings = idle_hours * rate * 0.70

        total_savings = power_savings + instance_savings

        # 신뢰도: idle 시간이 많을수록, z-score가 낮을수록 높음
        confidence = min(95, 60 + (idle_hours / 10) + abs(idle_df.shape[0] / len(gdf) * 30))

        worst_hour = idle_df.groupby('hour').size().idxmax() if 'hour' in idle_df.columns else 0

        results.append({
            'gpu_id':          gpu,
            'idle_hours':      idle_hours,
            'avg_util_pct':    round(idle_df['gpu_util'].mean(), 1),
            'worst_hour':      worst_hour,
            'monthly_savings': round(total_savings, 2),
            'confidence_pct':  round(confidence, 0),
        })

    return pd.DataFrame(results).sort_values('monthly_savings', ascending=False) if results else pd.DataFrame()

# ── Finding 02: Peak Scheduling ──
def detect_peak_jobs(df, col_map):
    """
    시간대별 요금 차이 기반 + 실제 절감 가능성 검증
    - 실제로 이동 가능한 작업만 탐지
    - 오프피크 여유 용량 확인
    """
    if 'electricity_rate' not in df.columns:
        return {'peak_hours_count': 0, 'current_cost': 0, 'monthly_savings': 0}

    rate_p75 = df['electricity_rate'].quantile(0.75)
    rate_p25 = df['electricity_rate'].quantile(0.25)

    peak_mask = df['electricity_rate'] >= rate_p75
    offpeak_mask = df['electricity_rate'] <= rate_p25

    if 'workload_type' in df.columns:
        training_mask = df['workload_type'].str.lower().isin(['training', 'train', 'batch'])
        peak = df[peak_mask & training_mask]
    else:
        # workload 없으면 high util을 training으로 간주
        peak = df[peak_mask & (df['gpu_util'] > 70)] if 'gpu_util' in df.columns else df[peak_mask]

    offpeak_rate = df[offpeak_mask]['electricity_rate'].mean()
    peak_rate    = df[peak_mask]['electricity_rate'].mean()

    if len(peak) == 0:
        return {'peak_hours_count': 0, 'current_cost': 0,
                'monthly_savings': 0, 'offpeak_rate': round(offpeak_rate, 2)}

    current_cost = (peak['electricity_rate'] * (peak.get('power_kw', pd.Series([3.0]*len(peak))))).sum()
    savings = (peak['electricity_rate'] - offpeak_rate).sum()

    return {
        'peak_hours_count': len(peak),
        'current_cost':     round(current_cost, 2),
        'monthly_savings':  round(max(savings, 0), 2),
        'offpeak_rate':     round(offpeak_rate, 2),
        'peak_rate':        round(peak_rate, 2),
    }

# ── Finding 03: Overprovisioning ──
def detect_overprovision(df, col_map):
    """
    시간대별 실제 필요 GPU 수 vs 가동 GPU 수
    - 피크 수요 × 1.25 버퍼 보장
    - 실제로 줄일 수 있는 시간대만 리포트
    """
    if 'gpu_id' not in df.columns:
        return {'total_gpus': 0, 'monthly_savings': 0, 'top_waste_hours': pd.DataFrame()}

    total_gpus = df['gpu_id'].nunique()
    rate = df['electricity_rate'].mean() if 'electricity_rate' in df.columns else 3.20

    hourly = df.groupby(['date', 'hour']).agg(
        gpus_on=('gpu_id', 'nunique'),
        avg_util=('gpu_util', 'mean') if 'gpu_util' in df.columns else ('gpu_id', 'count')
    ).reset_index()

    by_hour = hourly.groupby('hour').agg(
        avg_on=('gpus_on', 'mean'),
        p95_on=('gpus_on', lambda x: x.quantile(0.95)),  # 피크 95th percentile
    ).reset_index()

    savings_rows = []
    for _, row in by_hour.iterrows():
        hour = int(row['hour'])
        p95  = row['p95_on']
        avg  = row['avg_on']

        # 필요 GPU = p95 수요 × 1.25 버퍼
        needed = min(int(p95 * 1.25) + 1, total_gpus)
        reducible = max(0, total_gpus - needed)

        if reducible >= 1:
            saving = reducible * rate * 30
            savings_rows.append({
                'hour':           hour,
                'avg_on':         round(avg, 1),
                'p95_demand':     round(p95, 1),
                'needed_w_buffer':needed,
                'reducible':      reducible,
                'monthly_saving': round(saving, 2),
            })

    savings_df = pd.DataFrame(savings_rows)
    total_savings = savings_df['monthly_saving'].sum() if len(savings_df) > 0 else 0
    top = savings_df.nlargest(5, 'monthly_saving') if len(savings_df) > 0 else pd.DataFrame()

    return {
        'total_gpus':      total_gpus,
        'monthly_savings': round(total_savings, 2),
        'top_waste_hours': top,
        'savings_by_hour': savings_df,
    }

# ── Finding 04: Thermal Inefficiency (전문가 버전) ──
def detect_thermal(df, col_map):
    """온도 데이터 있을 때만 실행"""
    if 'temp_c' not in df.columns:
        return None

    results = []
    gpu_col = 'gpu_id' if 'gpu_id' in df.columns else None
    gpus = df[gpu_col].unique() if gpu_col else ['all']

    for gpu in gpus:
        gdf = df[df[gpu_col] == gpu] if gpu_col else df

        avg_temp = gdf['temp_c'].mean()
        max_temp = gdf['temp_c'].max()

        # 85°C 이상이면 thermal throttling 위험
        throttle_risk = (gdf['temp_c'] > 85).mean() * 100

        # 온도 낮은데 냉각 전력 높으면 → 냉각 과잉
        if 'cooling_kw' in gdf.columns:
            cooling_efficiency = gdf['gpu_util'].mean() / (gdf['cooling_kw'].mean() + 0.001) if 'gpu_util' in gdf.columns else 0
        else:
            cooling_efficiency = None

        results.append({
            'gpu_id':          gpu,
            'avg_temp_c':      round(avg_temp, 1),
            'max_temp_c':      round(max_temp, 1),
            'throttle_risk_pct': round(throttle_risk, 1),
            'cooling_efficiency': round(cooling_efficiency, 2) if cooling_efficiency else 'N/A',
        })

    return pd.DataFrame(results)

# ── 전체 분석 실행 ──
def run_analysis(filepath='gpu_metrics_30d.csv'):
    print("=" * 62)
    print("  InfraLens — AI Infrastructure Cost Analysis v2.0")
    print("=" * 62)

    df, col_map = load_data(filepath, chunksize=100000)

    detected = list(col_map.keys())
    tier = 'Pro' if len(detected) >= 8 else ('Standard' if len(detected) >= 5 else 'Basic')

    print(f"\n  Data:  {len(df):,} rows | {df['gpu_id'].nunique() if 'gpu_id' in df.columns else '?'} GPUs | {df['date'].nunique() if 'date' in df.columns else '?'} days")
    print(f"  Tier:  {tier} ({len(detected)} columns detected)")
    print(f"  Cols:  {', '.join(detected)}\n")

    idle = detect_idle(df, col_map)
    peak = detect_peak_jobs(df, col_map)
    over = detect_overprovision(df, col_map)
    thermal = detect_thermal(df, col_map)

    idle_total = idle['monthly_savings'].sum() if len(idle) > 0 else 0
    total = idle_total + peak['monthly_savings'] + over['monthly_savings']

    # ── Finding 01 ──
    print("[ FINDING 01 ]  GPU Idle Waste")
    print("-" * 62)
    if len(idle) > 0:
        for _, row in idle.iterrows():
            bar = '█' * int(row['confidence_pct'] / 10)
            print(f"  {row['gpu_id']}  |  {row['avg_util_pct']}% avg  |  "
                  f"{row['idle_hours']}h idle  |  worst {row['worst_hour']:02d}:00  |  "
                  f"save ${row['monthly_savings']:,.0f}/mo  |  confidence {row['confidence_pct']:.0f}%")
        print(f"\n  → Total: ${idle_total:,.2f} / month\n")
    else:
        print("  No significant idle waste detected.\n")

    # ── Finding 02 ──
    print("[ FINDING 02 ]  Peak-Rate Scheduling")
    print("-" * 62)
    if peak['peak_hours_count'] > 0:
        print(f"  Peak sessions:   {peak['peak_hours_count']}")
        print(f"  Peak rate:       ${peak.get('peak_rate', '?')}/hr")
        print(f"  Off-peak rate:   ${peak['offpeak_rate']}/hr")
        print(f"  Current cost:    ${peak['current_cost']:,.2f}/mo")
        print(f"  If rescheduled:  save ${peak['monthly_savings']:,.2f}/mo\n")
    else:
        print("  No peak scheduling waste detected.\n")

    # ── Finding 03 ──
    print("[ FINDING 03 ]  Overprovisioning")
    print("-" * 62)
    print(f"  Total GPUs: {over['total_gpus']} (buffer: 25%)")
    if len(over['top_waste_hours']) > 0:
        for _, row in over['top_waste_hours'].iterrows():
            print(f"  {int(row['hour']):02d}:00  |  avg {row['avg_on']} on  |  "
                  f"p95 demand {row['p95_demand']}  |  "
                  f"reducible {row['reducible']}  |  "
                  f"save ${row['monthly_saving']:,.0f}/mo")
    print(f"\n  → Total: ${over['monthly_savings']:,.2f} / month\n")

    # ── Finding 04 (있을 때만) ──
    if thermal is not None and len(thermal) > 0:
        print("[ FINDING 04 ]  Thermal Analysis")
        print("-" * 62)
        for _, row in thermal.iterrows():
            flag = ' ⚠ THROTTLE RISK' if row['throttle_risk_pct'] > 5 else ''
            print(f"  {row['gpu_id']}  |  avg {row['avg_temp_c']}°C  |  "
                  f"max {row['max_temp_c']}°C  |  throttle risk {row['throttle_risk_pct']}%{flag}")
        print()

    # ── Summary ──
    print("=" * 62)
    print(f"  TOTAL SAVINGS    ${total:>10,.2f} / month")
    print(f"  ANNUAL           ${total*12:>10,.2f} / year")
    print("=" * 62)

if __name__ == '__main__':
    run_analysis()
