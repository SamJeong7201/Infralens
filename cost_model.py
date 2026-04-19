import pandas as pd
import numpy as np

# 실제 TOU 요금 구조 (AWS, GCP, 한전 기반)
TOU_SCHEDULES = {
    'aws_us_east': {
        'peak':    {'hours': list(range(8, 22)),  'rate': 4.10},
        'offpeak': {'hours': list(range(0, 8)) + list(range(22, 24)), 'rate': 2.10},
        'weekend_discount': 0.15,
    },
    'gcp_us_central': {
        'peak':    {'hours': list(range(9, 21)),  'rate': 3.80},
        'offpeak': {'hours': list(range(0, 9)) + list(range(21, 24)), 'rate': 1.90},
        'weekend_discount': 0.10,
    },
    'kepco_korea': {
        'peak':    {'hours': list(range(10, 12)) + list(range(13, 17)), 'rate': 0.18},
        'mid':     {'hours': list(range(8, 10)) + list(range(12, 13)) + list(range(17, 23)), 'rate': 0.12},
        'offpeak': {'hours': list(range(0, 8)) + list(range(23, 24)), 'rate': 0.07},
        'weekend_discount': 0.20,
    },
    'auto': None  # 데이터에서 자동 감지
}

# GPU 모델별 TDP (최대 전력)
GPU_TDP = {
    'h100': 700, 'a100': 400, 'a100_40gb': 300,
    'v100': 300, 'a10':  150, 'a10g': 150,
    't4':   70,  'l4':   72,  'default': 300,
}

# 냉각 오버헤드 계수 (PUE - 1)
COOLING_OVERHEAD = {
    'hyperscale': 0.10,  # Google, AWS 최신 DC (PUE 1.1)
    'modern':     0.25,  # 일반 현대 DC (PUE 1.25)
    'average':    0.50,  # 업계 평균 (PUE 1.5)
    'old':        0.80,  # 노후 DC (PUE 1.8)
}

def get_hourly_rate(hour: int, is_weekend: bool, schedule: str = 'aws_us_east') -> float:
    """시간대별 전력 단가 계산"""
    if schedule == 'auto':
        return 3.20  # 기본값
    
    tou = TOU_SCHEDULES.get(schedule, TOU_SCHEDULES['aws_us_east'])
    
    # 기본 단가 결정
    if hour in tou['peak']['hours']:
        rate = tou['peak']['rate']
    elif 'mid' in tou and hour in tou['mid']['hours']:
        rate = tou['mid']['rate']
    else:
        rate = tou['offpeak']['rate']
    
    # 주말 할인
    if is_weekend:
        rate *= (1 - tou.get('weekend_discount', 0))
    
    return rate

def calculate_cooling_overhead(power_kw: float, dc_type: str = 'average') -> float:
    """냉각 오버헤드 전력 계산"""
    overhead_factor = COOLING_OVERHEAD.get(dc_type, 0.50)
    return power_kw * overhead_factor

def estimate_idle_power(gpu_model: str = 'default') -> float:
    """GPU idle 상태 전력 추정 (TDP의 ~15%)"""
    tdp = GPU_TDP.get(gpu_model.lower(), GPU_TDP['default'])
    return (tdp * 0.15) / 1000  # kW

def compute_efficiency_score(util: float, power_kw: float, 
                              gpu_model: str = 'default') -> float:
    """
    효율 점수 계산 (0~100)
    높을수록 좋음
    = 실제 작업량 / 소비 전력의 비율
    """
    if power_kw <= 0:
        return 0
    tdp_kw = GPU_TDP.get(gpu_model.lower(), GPU_TDP['default']) / 1000
    expected_power = (util / 100) * tdp_kw + estimate_idle_power(gpu_model)
    efficiency = min(100, (util / max(power_kw * 100 / tdp_kw, 1)) * 100)
    return round(efficiency, 1)

def detect_cost_type(df: pd.DataFrame) -> str:
    """
    cost_per_hr 컬럼이 인스턴스 요금인지 전력 요금인지 감지
    - 인스턴스 요금: ~/hr 범위 (AWS GPU 인스턴스)
    - 전력 요금: /bin/zsh.07~/bin/zsh.18/kWh 범위
    """
    if 'electricity_rate' in df.columns:
        col = df['electricity_rate']
    elif 'cost_per_hr' in df.columns:
        col = df['cost_per_hr']
    else:
        return 'instance'

    mean_rate = col.mean()
    if mean_rate > 0.5:
        return 'instance'  # 인스턴스 요금 ($/hr)
    else:
        return 'electricity'  # 전력 요금 ($/kWh)

def simulate_before_after(df: pd.DataFrame, 
                           idle_threshold: float = 20.0,
                           schedule: str = 'aws_us_east',
                           dc_type: str = 'average') -> dict:
    """
    Before/After 비용 시뮬레이션
    - Before: 현재 상태
    - After: 최적화 후 상태
    """
    df = df.copy()
    
    # 시간대별 요금 계산
    if 'hour' not in df.columns:
        df['hour'] = pd.to_datetime(df['timestamp']).dt.hour
    if 'is_weekend' not in df.columns:
        df['is_weekend'] = pd.to_datetime(df['timestamp']).dt.dayofweek >= 5

    cost_type = detect_cost_type(df)

    if cost_type == 'instance':
        # 인스턴스 요금 방식: cost_per_hr이 이미 전체 비용
        rate_col = 'electricity_rate' if 'electricity_rate' in df.columns else 'cost_per_hr'
        df['before_cost'] = df[rate_col]
        df['tou_rate'] = df[rate_col]

        # 냉각 오버헤드 추가
        overhead = COOLING_OVERHEAD.get(dc_type, 0.50)
        df['before_cost'] = df['before_cost'] * (1 + overhead * 0.3)

        # power_kw 없으면 추정
        if 'power_kw' not in df.columns and 'power_watt' in df.columns:
            df['power_kw'] = df['power_watt'] / 1000
        elif 'power_kw' not in df.columns:
            if 'gpu_model' in df.columns:
                gpu_model = df['gpu_model'].iloc[0]
            else:
                gpu_model = 'default'
            tdp_kw = GPU_TDP.get(str(gpu_model).lower(), GPU_TDP['default']) / 1000
            df['power_kw'] = df.get('gpu_util', pd.Series([50]*len(df))) / 100 * tdp_kw

    else:
        # 전력 요금 방식: power x rate 계산
        df['tou_rate'] = df.apply(
            lambda r: get_hourly_rate(r['hour'], r['is_weekend'], schedule), axis=1
        )

        if 'power_kw' not in df.columns:
            if 'power_watt' in df.columns:
                df['power_kw'] = df['power_watt'] / 1000
            else:
                gpu_model = df['gpu_model'].iloc[0] if 'gpu_model' in df.columns else 'default'
                tdp_kw = GPU_TDP.get(str(gpu_model).lower(), GPU_TDP['default']) / 1000
                df['power_kw'] = df.get('gpu_util', pd.Series([50]*len(df))) / 100 * tdp_kw

        df['cooling_kw']     = df['power_kw'].apply(lambda p: calculate_cooling_overhead(p, dc_type))
        df['total_power_kw'] = df['power_kw'] + df['cooling_kw']
        df['before_cost']    = df['total_power_kw'] * df['tou_rate']

    # AFTER: 최적화 적용
    # 1. Idle GPU → 절전 (70% 절감)
    idle_mask = df.get('util_rolling_3h', df['gpu_util']) < idle_threshold
    df['after_power_kw'] = df['power_kw'].copy()
    df.loc[idle_mask, 'after_power_kw'] = df.loc[idle_mask, 'power_kw'] * 0.30

    # 2. 피크 시간대 작업 → 오프피크로 이동 가능한 것들
    tou = TOU_SCHEDULES.get(schedule, TOU_SCHEDULES['aws_us_east'])
    peak_hours = set(tou['peak']['hours'])
    offpeak_rate = tou['offpeak']['rate']

    high_util_peak = (df['hour'].isin(peak_hours)) & (df['gpu_util'] > 70)
    df['after_rate'] = df['tou_rate'].copy()
    df.loc[high_util_peak, 'after_rate'] = offpeak_rate

    # 3. 냉각 재계산
    if cost_type == 'instance':
        # 인스턴스 방식: idle이면 70% 절감, peak shift면 offpeak rate 적용
        df['after_cost'] = df['before_cost'].copy()
        df.loc[idle_mask, 'after_cost'] = df.loc[idle_mask, 'before_cost'] * 0.30
        df.loc[high_util_peak, 'after_cost'] = df.loc[high_util_peak, 'before_cost'] * (offpeak_rate / df.loc[high_util_peak, 'tou_rate'].clip(lower=0.01))
    else:
        df['after_cooling_kw'] = df['after_power_kw'].apply(
            lambda p: calculate_cooling_overhead(p, dc_type)
        )
        df['after_total_kw'] = df['after_power_kw'] + df['after_cooling_kw']
        df['after_cost'] = df['after_total_kw'] * df['after_rate']

    # 결과 집계
    days = df['date'].nunique() if 'date' in df.columns else 30
    scale = 30 / max(days, 1)

    before_monthly = df['before_cost'].sum() * scale
    after_monthly  = df['after_cost'].sum() * scale
    savings_monthly = before_monthly - after_monthly
    savings_pct = (savings_monthly / before_monthly * 100) if before_monthly > 0 else 0

    # 시간대별 절감
    hourly_savings = df.groupby('hour').apply(
        lambda x: (x['before_cost'] - x['after_cost']).sum() * scale / days
    ).reset_index()
    hourly_savings.columns = ['hour', 'daily_savings']
    top_hours = hourly_savings.nlargest(5, 'daily_savings')

    return {
        'before_monthly':   round(before_monthly, 2),
        'after_monthly':    round(after_monthly, 2),
        'savings_monthly':  round(savings_monthly, 2),
        'savings_pct':      round(savings_pct, 1),
        'savings_annual':   round(savings_monthly * 12, 2),
        'top_saving_hours': top_hours,
        'df_simulated':     df,
        'schedule_used':    schedule,
        'dc_type':          dc_type,
    }

if __name__ == '__main__':
    from data_loader import load_and_prepare
    
    df, col_map, quality = load_and_prepare('gpu_metrics_30d.csv')
    
    print("=== Cost Model Test ===")
    print(f"TOU rate at 14:00 weekday: ${get_hourly_rate(14, False)}/hr")
    print(f"TOU rate at 03:00 weekday: ${get_hourly_rate(3, False)}/hr")
    print(f"TOU rate at 14:00 weekend: ${get_hourly_rate(14, True)}/hr")
    
    print("\n=== Before/After Simulation ===")
    result = simulate_before_after(df)
    print(f"Before:  ${result['before_monthly']:,.2f}/month")
    print(f"After:   ${result['after_monthly']:,.2f}/month")
    print(f"Savings: ${result['savings_monthly']:,.2f}/month ({result['savings_pct']}%)")
    print(f"Annual:  ${result['savings_annual']:,.2f}/year")
    print(f"\nTop saving hours:")
    print(result['top_saving_hours'].to_string(index=False))
