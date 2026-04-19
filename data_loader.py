import pandas as pd
import numpy as np
import os
import json
from dotenv import load_dotenv

load_dotenv()

COLUMN_MAP = {
    'timestamp':        ['timestamp', 'time', 'datetime', 'ts', 'date'],
    'gpu_id':           ['gpu_id', 'gpu', 'device_id', 'device', 'server_id'],
    'gpu_util':         ['gpu_util', 'gpu_util_pct', 'gpu_utilization', 'utilization',
                         'server_workload(%)', 'cpu_util', 'cpu_utilization'],
    'memory_util':      ['memory_util', 'mem_util', 'gpu_memory_pct', 'memory_utilization'],
    'power_kw':         ['power_kw', 'power_watt', 'power', 'watt', 'watts',
                         'power_consumption', 'cooling_unit_power_consumption(kw)'],
    'temp_c':           ['temp_c', 'temperature', 'temp', 'gpu_temp',
                         'inlet_temperature(°c)', 'inlet_temp'],
    'cooling_kw':       ['cooling_kw', 'cooling_power', 'cooling',
                         'cooling_unit_power_consumption(kw)'],
    'electricity_rate': ['electricity_rate', 'cost_per_hr', 'rate',
                         'total_energy_cost($)', 'energy_cost', 'price_per_kwh'],
    'workload_type':    ['workload_type', 'job_type', 'workload',
                         'cooling_strategy_action', 'task_type'],
    'gpu_model':        ['gpu_model', 'model', 'gpu_type', 'device_model'],
}

def ai_map_columns(columns: list) -> dict:
    try:
        from anthropic import Anthropic
        client = Anthropic()
        prompt = f"""You are an expert in GPU and data center infrastructure data analysis.

I have a CSV file with these column names:
{json.dumps(columns, indent=2)}

Map each column to one of these standard fields (use null if not relevant):
- timestamp: date/time of measurement
- gpu_id: GPU or server identifier  
- gpu_util: GPU/CPU utilization percentage (0-100)
- memory_util: memory utilization percentage (0-100)
- power_kw: power consumption in kW or W
- temp_c: temperature in Celsius
- cooling_kw: cooling power consumption
- electricity_rate: electricity cost or rate per hour
- workload_type: type of workload (training/inference/idle/batch)
- gpu_model: GPU hardware model

Respond ONLY with valid JSON: {{"standard_field": "original_column_name", ...}}
Use null for unmatched. Be smart — "Server_Workload(%)" maps to gpu_util."""

        msg = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=500,
            messages=[{"role": "user", "content": prompt}]
        )
        text = msg.content[0].text.strip()
        if '```' in text:
            text = text.split('```')[1]
            if text.startswith('json'):
                text = text[4:]
        mapping = json.loads(text)
        return {k: v for k, v in mapping.items() if v is not None}
    except Exception as e:
        print(f"AI mapping failed: {e}, using rule-based fallback")
        return {}

def rule_map_columns(df) -> dict:
    mapping = {}
    df_cols_lower = {c.lower().strip(): c for c in df.columns}
    for standard, variants in COLUMN_MAP.items():
        for v in variants:
            if v.lower() in df_cols_lower:
                mapping[standard] = df_cols_lower[v.lower()]
                break
    return mapping

def load_and_prepare(filepath, chunksize=None) -> tuple:
    """
    CSV 로드 → AI 컬럼 매핑 → 데이터 정제 → 파생 변수 생성
    Returns: (df, col_map, quality_report)
    """
    # 로드
    if chunksize:
        chunks = [c for c in pd.read_csv(filepath, chunksize=chunksize)]
        df = pd.concat(chunks, ignore_index=True)
    else:
        df = pd.read_csv(filepath)

    original_len = len(df)

    # nvidia-smi 형태 먼저 감지
    if detect_nvidia_smi(df):
        df = normalize_nvidia_smi(df)
        col_map = {c: c for c in df.columns if c in [
            'gpu_util','memory_util','power_kw','temp_c',
            'gpu_id','gpu_model','timestamp','electricity_rate'
        ]}
    else:
        # AI + 규칙 기반 컬럼 매핑
        api_key = os.getenv('ANTHROPIC_API_KEY')
        col_map = ai_map_columns(df.columns.tolist()) if api_key else {}
        rule_map = rule_map_columns(df)
        for k, v in rule_map.items():
            if k not in col_map:
                col_map[k] = v
        rename = {v: k for k, v in col_map.items()}
        df = df.rename(columns=rename)

    # timestamp 처리
    if 'timestamp' in df.columns:
        df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
        df = df.dropna(subset=['timestamp'])
        df = df.sort_values('timestamp').reset_index(drop=True)
        df['hour']    = df['timestamp'].dt.hour
        df['date']    = df['timestamp'].dt.date
        df['weekday'] = df['timestamp'].dt.dayofweek
        df['is_weekend'] = df['weekday'] >= 5
        df['is_business_hour'] = df['hour'].between(9, 18) & ~df['is_weekend']

    # power 단위 통일 (W → kW)
    if 'power_kw' in df.columns:
        if df['power_kw'].median() > 100:
            df['power_kw'] = df['power_kw'] / 1000

    # 결측값 처리
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    df[numeric_cols] = df[numeric_cols].fillna(df[numeric_cols].median())

    # 이상치 클리핑 (IQR 기반)
    if 'gpu_util' in df.columns:
        df['gpu_util'] = df['gpu_util'].clip(0, 100)
    if 'memory_util' in df.columns:
        df['memory_util'] = df['memory_util'].clip(0, 100)

    # GPU별 rolling 통계 추가
    if 'gpu_util' in df.columns:
        gpu_col = 'gpu_id' if 'gpu_id' in df.columns else None
        if gpu_col:
            df = df.sort_values([gpu_col, 'timestamp'])
            df['util_rolling_3h']  = df.groupby(gpu_col)['gpu_util'].transform(
                lambda x: x.rolling(3, min_periods=1).mean())
            df['util_rolling_24h'] = df.groupby(gpu_col)['gpu_util'].transform(
                lambda x: x.rolling(24, min_periods=1).mean())
            df['util_std_24h']     = df.groupby(gpu_col)['gpu_util'].transform(
                lambda x: x.rolling(24, min_periods=1).std().fillna(0))
        else:
            df['util_rolling_3h']  = df['gpu_util'].rolling(3, min_periods=1).mean()
            df['util_rolling_24h'] = df['gpu_util'].rolling(24, min_periods=1).mean()
            df['util_std_24h']     = df['gpu_util'].rolling(24, min_periods=1).std().fillna(0)

    # 데이터 품질 리포트
    quality = {
        'total_rows':    original_len,
        'clean_rows':    len(df),
        'columns_mapped': len(col_map),
        'tier': 'Pro' if len(col_map) >= 7 else ('Standard' if len(col_map) >= 4 else 'Basic'),
        'date_range':    f"{df['date'].min()} ~ {df['date'].max()}" if 'date' in df.columns else 'N/A',
        'devices':       df['gpu_id'].nunique() if 'gpu_id' in df.columns else 1,
    }

    return df, col_map, quality

if __name__ == '__main__':
    df, col_map, quality = load_and_prepare('gpu_metrics_30d.csv')
    print("=== Data Quality Report ===")
    for k, v in quality.items():
        print(f"  {k:20s}: {v}")
    print(f"\nColumns mapped: {list(col_map.keys())}")
    print(f"\nSample rolling stats:")
    if 'util_rolling_3h' in df.columns:
        print(df[['timestamp', 'gpu_util', 'util_rolling_3h', 'util_rolling_24h']].head(10).to_string())


def detect_nvidia_smi(df) -> bool:
    """nvidia-smi CSV 형태 감지"""
    nvidia_cols = ['utilization.gpu [%]', 'power.draw [W]',
                   'temperature.gpu', 'memory.used [MiB]']
    return any(c in df.columns for c in nvidia_cols)


def normalize_nvidia_smi(df) -> pd.DataFrame:
    """nvidia-smi CSV를 표준 형태로 변환"""
    rename_map = {}
    
    for col in df.columns:
        col_lower = col.lower().strip()
        if 'utilization.gpu' in col_lower:
            rename_map[col] = 'gpu_util'
        elif 'utilization.memory' in col_lower:
            rename_map[col] = 'memory_util'
        elif 'power.draw' in col_lower:
            rename_map[col] = 'power_kw'
        elif 'temperature.gpu' in col_lower:
            rename_map[col] = 'temp_c'
        elif 'memory.used' in col_lower:
            rename_map[col] = 'memory_used_mb'
        elif 'memory.total' in col_lower:
            rename_map[col] = 'memory_total_mb'
        elif 'timestamp' in col_lower:
            rename_map[col] = 'timestamp'
        elif col_lower == 'name' or 'gpu_name' in col_lower:
            rename_map[col] = 'gpu_model'
        elif 'index' in col_lower or col_lower == 'gpu':
            rename_map[col] = 'gpu_id'

    df = df.rename(columns=rename_map)

    # % 기호 제거
    for col in ['gpu_util', 'memory_util']:
        if col in df.columns:
            df[col] = df[col].astype(str).str.replace('%', '').str.strip()
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    # W → kW
    if 'power_kw' in df.columns:
        df['power_kw'] = df['power_kw'].astype(str).str.replace('W', '').str.strip()
        df['power_kw'] = pd.to_numeric(df['power_kw'], errors='coerce').fillna(0)
        if df['power_kw'].mean() > 10:
            df['power_kw'] = df['power_kw'] / 1000

    # MiB → 사용률 %
    if 'memory_used_mb' in df.columns and 'memory_total_mb' in df.columns:
        df['memory_used_mb'] = pd.to_numeric(
            df['memory_used_mb'].astype(str).str.replace('MiB','').str.strip(),
            errors='coerce').fillna(0)
        df['memory_total_mb'] = pd.to_numeric(
            df['memory_total_mb'].astype(str).str.replace('MiB','').str.strip(),
            errors='coerce').fillna(1)
        if 'memory_util' not in df.columns:
            df['memory_util'] = (df['memory_used_mb'] / df['memory_total_mb'] * 100).round(1)

    # gpu_id가 숫자면 이름으로 변환
    if 'gpu_id' in df.columns:
        df['gpu_id'] = df['gpu_id'].astype(str).str.strip()
        df['gpu_id'] = df['gpu_id'].apply(
            lambda x: f'gpu-{x}' if x.isdigit() else x)
    
    # 기본 전력 요금 추가 (없으면)
    if 'electricity_rate' not in df.columns:
        if 'hour' in df.columns:
            df['electricity_rate'] = df['hour'].apply(
                lambda h: 4.10 if 8 <= h < 22 else 2.10)
        else:
            df['electricity_rate'] = 3.20

    return df
