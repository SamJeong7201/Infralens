import pandas as pd
import numpy as np
from scipy import stats

def detect_idle_advanced(df: pd.DataFrame) -> pd.DataFrame:
    """
    고정밀 Idle 탐지
    1. 단순 임계값 X
    2. Rolling average 기반
    3. 같은 요일/시간대 baseline 비교
    4. Z-score 이상 탐지
    5. 신뢰도 점수
    """
    results = []
    gpu_col = 'gpu_id' if 'gpu_id' in df.columns else None
    gpus = df[gpu_col].unique() if gpu_col else ['all']

    for gpu in gpus:
        gdf = df[df[gpu_col] == gpu].copy() if gpu_col else df.copy()
        if 'gpu_util' not in gdf.columns or len(gdf) < 10:
            continue

        # 1. 시간대별 baseline (같은 시간 평균)
        hourly_baseline = gdf.groupby('hour')['gpu_util'].agg(['mean', 'std']).reset_index()
        hourly_baseline.columns = ['hour', 'baseline_mean', 'baseline_std']
        hourly_baseline['baseline_std'] = hourly_baseline['baseline_std'].fillna(5)
        gdf = gdf.merge(hourly_baseline, on='hour', how='left')

        # 2. Z-score (현재 vs 시간대 baseline)
        gdf['z_score'] = (gdf['gpu_util'] - gdf['baseline_mean']) / gdf['baseline_std'].clip(lower=1)

        # 3. Rolling 기반 idle 판단
        rolling_col = 'util_rolling_3h' if 'util_rolling_3h' in gdf.columns else 'gpu_util'

        # 4. 복합 조건으로 idle 탐지
        idle_mask = (
            (gdf[rolling_col] < 25) &          # rolling avg 낮음
            (gdf['z_score'] < -0.5) &           # 평소보다 낮음
            (gdf['gpu_util'] < 35)              # 현재 사용률 낮음
        )
        idle_rows = gdf[idle_mask]

        if len(idle_rows) == 0:
            continue

        # 5. 시간대별 그룹핑
        idle_by_hour = idle_rows.groupby('hour').agg(
            count=('gpu_util', 'count'),
            avg_util=('gpu_util', 'mean'),
            avg_power=('power_kw', 'mean') if 'power_kw' in idle_rows.columns else ('gpu_util', 'count'),
        ).reset_index()

        # 6. 절감액 계산
        rate = idle_rows['electricity_rate'].mean() if 'electricity_rate' in idle_rows.columns else 3.20
        power_avg = idle_rows['power_kw'].mean() if 'power_kw' in idle_rows.columns else 0.3
        idle_hours_total = len(idle_rows)
        savings = idle_hours_total * rate * 0.70  # 70% 절감

        # 7. 신뢰도 점수
        consistency = (idle_rows['z_score'] < -1.0).mean()
        confidence = min(95, 50 + consistency * 40 + min(idle_hours_total / 5, 5))

        worst_hour = idle_rows.groupby('hour').size().idxmax()

        results.append({
            'gpu_id':          gpu,
            'idle_hours':      idle_hours_total,
            'avg_util_pct':    round(idle_rows['gpu_util'].mean(), 1),
            'avg_power_kw':    round(power_avg, 3),
            'worst_hour':      worst_hour,
            'idle_by_hour':    idle_by_hour,
            'monthly_savings': round(savings, 2),
            'confidence_pct':  round(confidence, 1),
        })

    return pd.DataFrame(results).sort_values('monthly_savings', ascending=False) if results else pd.DataFrame()


def detect_peak_waste_advanced(df: pd.DataFrame, schedule: str = 'aws_us_east') -> dict:
    """
    피크 요금 낭비 탐지
    - 피크 시간대 고부하 작업 탐지
    - 오프피크로 이동 가능한 작업 식별
    - 실제 절감 가능액 계산
    """
    from cost_model import TOU_SCHEDULES, get_hourly_rate

    tou = TOU_SCHEDULES.get(schedule, TOU_SCHEDULES['aws_us_east'])
    peak_hours = set(tou['peak']['hours'])
    offpeak_rate = tou['offpeak']['rate']

    if 'electricity_rate' in df.columns:
        rate_p75 = df['electricity_rate'].quantile(0.75)
        peak_mask = df['electricity_rate'] >= rate_p75
    else:
        peak_mask = df['hour'].isin(peak_hours)

    # 이동 가능한 작업: 피크 시간 + 높은 사용률
    if 'workload_type' in df.columns:
        movable = df[peak_mask & df['workload_type'].str.lower().str.contains(
            'train|batch|job', na=False)]
    else:
        movable = df[peak_mask & (df['gpu_util'] > 65)]

    if len(movable) == 0:
        return {'peak_hours_count': 0, 'current_cost': 0,
                'monthly_savings': 0, 'offpeak_rate': offpeak_rate}

    # 현재 비용 vs 이동 후 비용
    current_rate = movable['electricity_rate'].mean() if 'electricity_rate' in movable.columns else tou['peak']['rate']
    current_cost = movable['electricity_rate'].sum() if 'electricity_rate' in movable.columns else len(movable) * current_rate
    savings = (movable['electricity_rate'] - offpeak_rate).sum() if 'electricity_rate' in movable.columns else len(movable) * (current_rate - offpeak_rate)

    # 일별 피크 패턴
    peak_by_hour = movable.groupby('hour').size().reset_index(name='count')

    days = df['date'].nunique() if 'date' in df.columns else 30
    scale = 30 / max(days, 1)

    return {
        'peak_hours_count': len(movable),
        'current_cost':     round(current_cost * scale, 2),
        'monthly_savings':  round(max(savings, 0) * scale, 2),
        'offpeak_rate':     round(offpeak_rate, 2),
        'peak_rate':        round(current_rate, 2),
        'peak_by_hour':     peak_by_hour,
    }


def detect_overprovision_advanced(df: pd.DataFrame) -> dict:
    """
    과잉 프로비저닝 탐지
    - 시간대별 실제 필요 GPU 수 계산
    - p95 수요 기반 (안전 마진 포함)
    - 절감 가능 GPU 수 및 비용 계산
    """
    if 'gpu_id' not in df.columns:
        return {'total_gpus': 0, 'monthly_savings': 0, 'top_waste_hours': pd.DataFrame()}

    total_gpus = df['gpu_id'].nunique()
    rate = df['electricity_rate'].mean() if 'electricity_rate' in df.columns else 3.20

    # 시간대별 활성 GPU (사용률 > 15%)
    active = df[df['gpu_util'] > 15].groupby(
        ['date', 'hour']
    )['gpu_id'].nunique().reset_index(name='active_gpus')

    by_hour = active.groupby('hour').agg(
        avg_active=('active_gpus', 'mean'),
        p95_active=('active_gpus', lambda x: x.quantile(0.95)),
        max_active=('active_gpus', 'max'),
    ).reset_index()

    rows = []
    for _, row in by_hour.iterrows():
        # 필요 GPU = p95 수요 × 1.20 버퍼
        needed = min(int(row['p95_active'] * 1.20) + 1, total_gpus)
        reducible = max(0, total_gpus - needed)

        if reducible >= 1:
            saving = reducible * rate * 30
            rows.append({
                'hour':            int(row['hour']),
                'avg_active':      round(row['avg_active'], 1),
                'p95_demand':      round(row['p95_active'], 1),
                'max_demand':      int(row['max_active']),
                'needed_w_buffer': needed,
                'reducible':       reducible,
                'monthly_saving':  round(saving, 2),
            })

    savings_df = pd.DataFrame(rows)
    total = savings_df['monthly_saving'].sum() if len(savings_df) > 0 else 0
    top = savings_df.nlargest(5, 'monthly_saving') if len(savings_df) > 0 else pd.DataFrame()

    return {
        'total_gpus':      total_gpus,
        'monthly_savings': round(total, 2),
        'top_waste_hours': top,
        'savings_by_hour': savings_df,
    }


def compute_efficiency_scores(df: pd.DataFrame) -> pd.DataFrame:
    """
    GPU별 효율 점수 계산
    - 사용률 대비 전력 효율
    - 시간대별 효율 패턴
    - 전체 점수 (0~100)
    """
    gpu_col = 'gpu_id' if 'gpu_id' in df.columns else None
    gpus = df[gpu_col].unique() if gpu_col else ['all']
    results = []

    for gpu in gpus:
        gdf = df[df[gpu_col] == gpu] if gpu_col else df

        util_mean = gdf['gpu_util'].mean() if 'gpu_util' in gdf.columns else 0
        util_std  = gdf['gpu_util'].std()  if 'gpu_util' in gdf.columns else 0

        # 효율 점수 구성
        util_score    = min(100, util_mean * 1.2)        # 사용률 점수
        consist_score = max(0, 100 - util_std * 2)       # 일관성 점수
        waste_hours   = (gdf['gpu_util'] < 15).mean() * 100 if 'gpu_util' in gdf.columns else 0
        waste_score   = max(0, 100 - waste_hours * 2)    # 낭비 없음 점수

        total_score = (util_score * 0.4 + consist_score * 0.3 + waste_score * 0.3)

        grade = 'A' if total_score >= 80 else \
                'B' if total_score >= 65 else \
                'C' if total_score >= 50 else 'D'

        results.append({
            'gpu_id':       gpu,
            'efficiency':   round(total_score, 1),
            'grade':        grade,
            'avg_util':     round(util_mean, 1),
            'util_std':     round(util_std, 1),
            'waste_pct':    round(waste_hours, 1),
        })

    return pd.DataFrame(results).sort_values('efficiency', ascending=False)


def run_full_analysis(filepath='gpu_metrics_30d.csv', schedule='aws_us_east', dc_type='average'):
    from data_loader import load_and_prepare
    from cost_model import simulate_before_after

    df, col_map, quality = load_and_prepare(filepath)

    print("=" * 65)
    print("  InfraLens — Advanced Analysis Engine v4.0")
    print("=" * 65)
    print(f"\n  {quality['tier']} tier | {quality['clean_rows']:,} rows | "
          f"{quality['devices']} devices | {quality['date_range']}\n")

    idle    = detect_idle_advanced(df)
    peak    = detect_peak_waste_advanced(df, schedule)
    over    = detect_overprovision_advanced(df)
    scores  = compute_efficiency_scores(df)
    sim     = simulate_before_after(df, schedule=schedule, dc_type=dc_type)

    idle_total = idle['monthly_savings'].sum() if len(idle) > 0 else 0

    # ── Efficiency Scores ──
    print("[ EFFICIENCY SCORES ]")
    print("-" * 65)
    for _, row in scores.iterrows():
        bar = '█' * int(row['efficiency'] / 10)
        print(f"  {row['gpu_id']:10s}  Grade {row['grade']}  {bar:10s} {row['efficiency']:.0f}  "
              f"avg {row['avg_util']}%  waste {row['waste_pct']}%")
    print()

    # ── Finding 01 ──
    print("[ FINDING 01 ]  Idle Waste (Rolling Average + Z-score)")
    print("-" * 65)
    if len(idle) > 0:
        for _, row in idle.iterrows():
            print(f"  {row['gpu_id']:10s} | {row['avg_util_pct']}% avg util | "
                  f"{row['idle_hours']}h idle | worst {row['worst_hour']:02d}:00 | "
                  f"${row['monthly_savings']:,.0f}/mo | {row['confidence_pct']:.0f}% conf")
        print(f"\n  → Total: ${idle_total:,.2f}/month\n")
    else:
        print("  No significant idle waste.\n")

    # ── Finding 02 ──
    print("[ FINDING 02 ]  Peak-Rate Scheduling")
    print("-" * 65)
    if peak['peak_hours_count'] > 0:
        print(f"  Sessions in peak window: {peak['peak_hours_count']}")
        print(f"  Peak rate:    ${peak['peak_rate']}/hr")
        print(f"  Off-peak:     ${peak['offpeak_rate']}/hr")
        print(f"  Current cost: ${peak['current_cost']:,.2f}/mo")
        print(f"  Savings:      ${peak['monthly_savings']:,.2f}/mo\n")
    else:
        print("  No peak waste.\n")

    # ── Finding 03 ──
    print("[ FINDING 03 ]  Overprovisioning (p95 demand)")
    print("-" * 65)
    if over['monthly_savings'] > 0 and len(over['top_waste_hours']) > 0:
        for _, row in over['top_waste_hours'].iterrows():
            print(f"  {int(row['hour']):02d}:00 | avg {row['avg_active']} on | "
                  f"p95 {row['p95_demand']} | reducible {row['reducible']} | "
                  f"${row['monthly_saving']:,.0f}/mo")
        print(f"\n  → Total: ${over['monthly_savings']:,.2f}/month\n")
    else:
        print("  No overprovisioning detected.\n")

    # ── Before/After ──
    print("[ BEFORE / AFTER SIMULATION ]")
    print("-" * 65)
    print(f"  Schedule:  {sim['schedule_used']} | DC type: {sim['dc_type']}")
    print(f"  Before:    ${sim['before_monthly']:>10,.2f} / month")
    print(f"  After:     ${sim['after_monthly']:>10,.2f} / month")
    print(f"  Savings:   ${sim['savings_monthly']:>10,.2f} / month  ({sim['savings_pct']}%)")
    print(f"  Annual:    ${sim['savings_annual']:>10,.2f} / year")

    print("\n  Top saving hours:")
    for _, row in sim['top_saving_hours'].iterrows():
        print(f"    {int(row['hour']):02d}:00 → save ${row['daily_savings']:.2f}/day")

    print("\n" + "=" * 65)
    print(f"  TOTAL OPPORTUNITY   ${sim['savings_monthly']:>10,.2f} / month")
    print(f"  ANNUAL OPPORTUNITY  ${sim['savings_annual']:>10,.2f} / year")
    print(f"  SAVINGS RATE        {sim['savings_pct']}%")
    print("=" * 65)

if __name__ == '__main__':
    run_full_analysis()


def run_billing_analysis(df, col_map):
    """빌링 데이터 전용 분석 — data_profiler 연동"""
    from data_profiler import analyze_billing
    return analyze_billing(df, col_map)


def detect_idle_ml(df: pd.DataFrame) -> pd.DataFrame:
    """
    2단계: Isolation Forest 기반 이상 탐지
    - 단순 임계값이나 Z-score로 못 잡는 복합 패턴 탐지
    - 다변수 (gpu_util + power_kw + hour + memory_util) 동시 분석
    - 각 포인트의 이상 점수 계산
    """
    from sklearn.ensemble import IsolationForest
    from sklearn.preprocessing import StandardScaler

    results = []
    gpu_col = 'gpu_id' if 'gpu_id' in df.columns else None
    gpus = df[gpu_col].unique() if gpu_col else ['all']

    # 사용할 피처 선택
    feature_cols = []
    for col in ['gpu_util', 'power_kw', 'memory_util', 'hour', 'util_rolling_3h']:
        if col in df.columns:
            feature_cols.append(col)

    if len(feature_cols) < 2:
        return pd.DataFrame()

    for gpu in gpus:
        gdf = df[df[gpu_col] == gpu].copy() if gpu_col else df.copy()

        if len(gdf) < 20:
            continue

        # 피처 행렬 준비
        X = gdf[feature_cols].fillna(gdf[feature_cols].median())

        # 정규화
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X)

        # Isolation Forest
        clf = IsolationForest(
            contamination=0.15,  # 15%가 이상치라고 가정
            random_state=42,
            n_estimators=100
        )
        gdf = gdf.copy()
        gdf['anomaly_score'] = clf.fit_predict(X_scaled)
        gdf['anomaly_raw']   = clf.score_samples(X_scaled)

        # 이상 포인트 = -1
        anomalies = gdf[gdf['anomaly_score'] == -1]

        # idle 이상치만 (낮은 사용률)
        if 'gpu_util' in anomalies.columns:
            idle_anomalies = anomalies[anomalies['gpu_util'] < 35]
        else:
            idle_anomalies = anomalies

        if len(idle_anomalies) == 0:
            continue

        idle_hours = len(idle_anomalies)
        rate = idle_anomalies['electricity_rate'].mean() if 'electricity_rate' in idle_anomalies.columns else 3.20
        savings = idle_hours * rate * 0.70
        confidence = min(95, 65 + (idle_hours / len(gdf)) * 100)
        worst_hour = idle_anomalies.groupby('hour').size().idxmax() if 'hour' in idle_anomalies.columns else 0

        results.append({
            'gpu_id':           gpu,
            'idle_hours':       idle_hours,
            'avg_util_pct':     round(idle_anomalies['gpu_util'].mean(), 1) if 'gpu_util' in idle_anomalies.columns else 0,
            'worst_hour':       worst_hour,
            'monthly_savings':  round(savings, 2),
            'confidence_pct':   round(confidence, 1),
            'method':           'Isolation Forest (ML)',
        })

    return pd.DataFrame(results).sort_values('monthly_savings', ascending=False) if results else pd.DataFrame()


def detect_idle_combined(df: pd.DataFrame) -> pd.DataFrame:
    """
    1단계 (Z-score) + 2단계 (Isolation Forest) 결합
    - 두 방법 모두 잡은 것: 높은 신뢰도
    - 한 방법만 잡은 것: 중간 신뢰도
    - 결과 병합 및 신뢰도 조정
    """
    rule_results = detect_idle_advanced(df)
    ml_results   = detect_idle_ml(df)

    if len(rule_results) == 0 and len(ml_results) == 0:
        return pd.DataFrame()

    if len(rule_results) == 0:
        ml_results['detection_method'] = 'ML only'
        return ml_results

    if len(ml_results) == 0:
        rule_results['detection_method'] = 'Rule + Z-score'
        return rule_results

    # 두 결과 병합
    merged = rule_results.merge(
        ml_results[['gpu_id', 'idle_hours', 'monthly_savings', 'confidence_pct']],
        on='gpu_id', how='outer', suffixes=('_rule', '_ml')
    )

    final = []
    for _, row in merged.iterrows():
        rule_savings = row.get('monthly_savings_rule', 0) or 0
        ml_savings   = row.get('monthly_savings_ml', 0)   or 0
        rule_conf    = row.get('confidence_pct_rule', 0)  or 0
        ml_conf      = row.get('confidence_pct_ml', 0)    or 0

        if rule_savings > 0 and ml_savings > 0:
            # 둘 다 잡음 → 높은 신뢰도
            savings    = (rule_savings + ml_savings) / 2
            confidence = min(95, (rule_conf + ml_conf) / 2 + 10)
            method     = 'Rule + ML (high confidence)'
        elif rule_savings > 0:
            savings    = rule_savings
            confidence = rule_conf
            method     = 'Rule-based'
        else:
            savings    = ml_savings
            confidence = ml_conf * 0.85
            method     = 'ML only'

        final.append({
            'gpu_id':           row['gpu_id'],
            'idle_hours':       row.get('idle_hours_rule') or row.get('idle_hours_ml', 0),
            'avg_util_pct':     row.get('avg_util_pct', 0),
            'worst_hour':       row.get('worst_hour', 0),
            'monthly_savings':  round(savings, 2),
            'confidence_pct':   round(confidence, 1),
            'detection_method': method,
        })

    result_df = pd.DataFrame(final)
    return result_df.sort_values('monthly_savings', ascending=False)
