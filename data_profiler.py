import pandas as pd
import numpy as np

def profile_dataset(df) -> dict:
    """
    데이터 타입 자동 감지
    Returns: profile dict with data_type, characteristics
    """
    profile = {
        'data_type': None,
        'has_timestamp': False,
        'is_timeseries': False,
        'is_billing': False,
        'row_count': len(df),
        'characteristics': []
    }

    cols_lower = [c.lower() for c in df.columns]

    # 1. timestamp 있는지
    time_cols = ['timestamp', 'time', 'datetime', 'date',
                 'usage start date', 'usage_start_date', 'start_time']
    profile['has_timestamp'] = any(c in cols_lower for c in time_cols)

    # 2. 시계열 판단 — 같은 리소스가 여러 시간대에 반복 등장
    if 'gpu_id' in df.columns or 'resource id' in df.columns.str.lower().tolist():
        id_col = 'gpu_id' if 'gpu_id' in df.columns else [
            c for c in df.columns if c.lower() == 'resource id'][0]
        unique_ids = df[id_col].nunique()
        rows_per_id = len(df) / max(unique_ids, 1)
        profile['is_timeseries'] = rows_per_id >= 12  # 12시간 이상 반복

    # 3. 빌링 레코드 판단
    billing_keywords = ['cost', 'billing', 'invoice', 'charge',
                        'unrounded', 'rounded', 'inr', 'usd']
    billing_cols = sum(1 for c in cols_lower if any(k in c for k in billing_keywords))
    profile['is_billing'] = billing_cols >= 2

    # 4. 최종 타입 결정
    if profile['is_timeseries'] and not profile['is_billing']:
        profile['data_type'] = 'timeseries'
        profile['characteristics'].append('Time-series monitoring data')
    elif profile['is_billing']:
        profile['data_type'] = 'billing'
        profile['characteristics'].append('Billing/invoice records')
    elif profile['has_timestamp'] and not profile['is_timeseries']:
        profile['data_type'] = 'sparse_timeseries'
        profile['characteristics'].append('Sparse time-series (few readings per resource)')
    else:
        profile['data_type'] = 'unknown'
        profile['characteristics'].append('Unknown format')

    # 5. 추가 특성
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    if len(numeric_cols) > 0:
        profile['characteristics'].append(f'{len(numeric_cols)} numeric columns')

    return profile


def analyze_billing(df, col_map) -> dict:
    """
    빌링 레코드 전용 분석
    - 서비스별 비용 분석
    - 고비용 리소스 탐지
    - 사용량 대비 비용 효율
    - 최적화 기회 탐지
    """
    results = {
        'monthly_savings': 0,
        'findings': [],
        'top_cost_resources': pd.DataFrame(),
        'service_breakdown': pd.DataFrame(),
        'optimization_opportunities': []
    }

    # 비용 컬럼 찾기
    cost_col = None
    for col in df.columns:
        if any(k in col.lower() for k in ['unrounded cost', 'cost ($)', 'rounded cost']):
            cost_col = col
            break
    if cost_col is None and 'electricity_rate' in df.columns:
        cost_col = 'electricity_rate'

    if cost_col is None:
        return results

    df = df.copy()
    df[cost_col] = pd.to_numeric(df[cost_col], errors='coerce').fillna(0)

    # 서비스명 컬럼 찾기
    service_col = None
    for col in df.columns:
        if 'service' in col.lower():
            service_col = col
            break

    # 리소스 ID 컬럼
    id_col = 'gpu_id' if 'gpu_id' in df.columns else None
    if id_col is None:
        for col in df.columns:
            if 'resource' in col.lower() or 'id' in col.lower():
                id_col = col
                break

    total_cost = df[cost_col].sum()
    days = 30  # 기본값

    # timestamp로 실제 기간 계산
    if 'timestamp' in df.columns:
        try:
            df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
            date_range = (df['timestamp'].max() - df['timestamp'].min()).days
            if date_range > 0:
                days = date_range
        except:
            pass

    monthly_cost = total_cost * (30 / max(days, 1))

    # 1. 서비스별 비용 분석
    if service_col:
        service_breakdown = df.groupby(service_col)[cost_col].sum().reset_index()
        service_breakdown.columns = ['service', 'cost']
        service_breakdown = service_breakdown.sort_values('cost', ascending=False)
        service_breakdown['pct'] = (service_breakdown['cost'] / total_cost * 100).round(1)
        results['service_breakdown'] = service_breakdown

    # 2. 고비용 리소스 탐지
    if id_col:
        resource_costs = df.groupby(id_col)[cost_col].sum().reset_index()
        resource_costs.columns = ['resource_id', 'total_cost']
        resource_costs = resource_costs.sort_values('total_cost', ascending=False)

        # 상위 20% 리소스가 전체 비용의 80% 차지하는지 확인 (파레토 법칙)
        top_20_pct = resource_costs.head(max(1, len(resource_costs) // 5))
        top_cost_pct = top_20_pct['total_cost'].sum() / total_cost * 100

        results['top_cost_resources'] = resource_costs.head(10)

        if top_cost_pct > 60:
            savings = resource_costs.head(3)['total_cost'].sum() * 0.20 * (30 / max(days, 1))
            results['findings'].append({
                'type': 'Cost Concentration',
                'title': f'Top 20% of resources drive {top_cost_pct:.0f}% of costs',
                'detail': f'High cost concentration detected. Top 3 resources: '
                         f'{resource_costs.head(3)[id_col].tolist()}',
                'action': 'Review top cost resources for rightsizing or reservation opportunities. '
                         'Consider Reserved Instances or Committed Use Discounts.',
                'monthly_savings': round(savings, 2),
                'confidence': 80.0,
                'effort': 'Medium',
                'timeframe': 'This month'
            })
            results['monthly_savings'] += savings

    # 3. CPU 사용률 기반 최적화
    util_col = 'gpu_util' if 'gpu_util' in df.columns else None
    if util_col:
        df[util_col] = pd.to_numeric(df[util_col], errors='coerce').fillna(50)
        low_util = df[df[util_col] < 30]

        if len(low_util) > 0:
            low_util_cost = low_util[cost_col].sum() * (30 / max(days, 1))
            savings = low_util_cost * 0.40

            results['findings'].append({
                'type': 'Low Utilization',
                'title': f'{len(low_util)} resources with <30% CPU utilization',
                'detail': f'Low utilization resources consuming ${low_util_cost:,.2f}/month. '
                         f'Average utilization: {low_util[util_col].mean():.1f}%',
                'action': 'Rightsize or consolidate low-utilization resources. '
                         'Consider smaller instance types or spot instances.',
                'monthly_savings': round(savings, 2),
                'confidence': 75.0,
                'effort': 'Low',
                'timeframe': 'This week'
            })
            results['monthly_savings'] += savings

    # 4. 지역별 비용 분석
    region_col = None
    for col in df.columns:
        if 'region' in col.lower() or 'zone' in col.lower():
            region_col = col
            break

    if region_col:
        region_costs = df.groupby(region_col)[cost_col].sum().reset_index()
        region_costs.columns = ['region', 'cost']
        region_costs = region_costs.sort_values('cost', ascending=False)

        if len(region_costs) > 1:
            expensive_region = region_costs.iloc[0]
            cheap_region = region_costs.iloc[-1]
            if expensive_region['cost'] > cheap_region['cost'] * 2:
                savings = expensive_region['cost'] * 0.15 * (30 / max(days, 1))
                results['findings'].append({
                    'type': 'Regional Optimization',
                    'title': f'Cost imbalance across {len(region_costs)} regions',
                    'detail': f'Most expensive region: {expensive_region["region"]} '
                             f'(${expensive_region["cost"]:,.2f}). '
                             f'Consider workload migration to cheaper regions.',
                    'action': f'Evaluate migrating non-latency-sensitive workloads '
                             f'from {expensive_region["region"]} to lower-cost regions.',
                    'monthly_savings': round(savings, 2),
                    'confidence': 65.0,
                    'effort': 'High',
                    'timeframe': 'This month'
                })
                results['monthly_savings'] += savings

    results['monthly_cost'] = round(monthly_cost, 2)
    results['total_cost_in_data'] = round(total_cost, 2)
    results['findings'] = sorted(results['findings'],
                                  key=lambda x: x['monthly_savings'], reverse=True)

    return results


if __name__ == '__main__':
    from data_loader import load_and_prepare

    print("=== Testing with GCP billing data ===")
    df, col_map, quality = load_and_prepare('gcp_final_approved_dataset.csv')
    profile = profile_dataset(df)
    print(f"Data type: {profile['data_type']}")
    print(f"Characteristics: {profile['characteristics']}")

    if profile['data_type'] == 'billing':
        billing = analyze_billing(df, col_map)
        print(f"\nMonthly cost: ${billing['monthly_cost']:,.2f}")
        print(f"Monthly savings: ${billing['monthly_savings']:,.2f}")
        print(f"\nFindings:")
        for f in billing['findings']:
            print(f"  [{f['type']}] {f['title']}")
            print(f"  Savings: ${f['monthly_savings']:,.2f}/mo | {f['effort']} effort")
            print(f"  Action: {f['action'][:80]}...")
            print()

    print("\n=== Testing with GPU timeseries data ===")
    df2, col_map2, quality2 = load_and_prepare('gpu_metrics_30d.csv')
    profile2 = profile_dataset(df2)
    print(f"Data type: {profile2['data_type']}")
    print(f"Characteristics: {profile2['characteristics']}")
