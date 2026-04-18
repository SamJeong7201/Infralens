import pandas as pd
import numpy as np
import os
import json
from dotenv import load_dotenv

load_dotenv()

COLUMN_MAP = {
    'timestamp':       ['timestamp', 'time', 'datetime', 'ts', 'date'],
    'gpu_id':          ['gpu_id', 'gpu', 'device_id', 'device', 'server_id'],
    'gpu_util':        ['gpu_util', 'gpu_util_pct', 'gpu_utilization', 'util', 'server_workload(%)'],
    'memory_util':     ['memory_util', 'mem_util', 'gpu_memory_pct'],
    'power_kw':        ['power_kw', 'power_watt', 'power', 'watt', 'watts'],
    'temp_c':          ['temp_c', 'temperature', 'temp', 'inlet_temperature(°c)'],
    'cooling_kw':      ['cooling_kw', 'cooling_power', 'cooling_unit_power_consumption(kw)'],
    'electricity_rate':['electricity_rate', 'cost_per_hr', 'rate', 'total_energy_cost($)'],
    'workload_type':   ['workload_type', 'job_type', 'workload', 'cooling_strategy_action'],
    'gpu_model':       ['gpu_model', 'model', 'gpu_type'],
}

def ai_map_columns(columns: list) -> dict:
    """AI 기반 컬럼 자동 매핑"""
    try:
        from anthropic import Anthropic
        client = Anthropic()
        prompt = f"""You are an expert in data center and GPU infrastructure data.
I have a CSV file with these column names:
{json.dumps(columns, indent=2)}

Map each column to one of these standard fields (or null if not relevant):
- timestamp, gpu_id, gpu_util, memory_util, power_kw, temp_c, cooling_kw, electricity_rate, workload_type

Respond with ONLY a valid JSON object mapping standard_field -> original_column_name.
Use null for unmatched fields."""

        message = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=500,
            messages=[{"role": "user", "content": prompt}]
        )
        text = message.content[0].text.strip()
        if '```' in text:
            text = text.split('```')[1]
            if text.startswith('json'):
                text = text[4:]
        mapping = json.loads(text)
        return {k: v for k, v in mapping.items() if v is not None}
    except Exception as e:
        return {}

def detect_columns(df):
    """규칙 기반 컬럼 매핑 (fallback)"""
    mapping = {}
    df_cols_lower = {c.lower(): c for c in df.columns}
    for standard, variants in COLUMN_MAP.items():
        for v in variants:
            if v.lower() in df_cols_lower:
                mapping[standard] = df_cols_lower[v.lower()]
                break
    return mapping

def load_data(filepath, chunksize=None):
    if chunksize:
        chunks = [chunk for chunk in pd.read_csv(filepath, chunksize=chunksize)]
        df = pd.concat(chunks, ignore_index=True)
    else:
        df = pd.read_csv(filepath)

    # 1. AI 매핑 시도
    api_key = os.getenv('ANTHROPIC_API_KEY')
    if api_key:
        col_map = ai_map_columns(df.columns.tolist())
    else:
        col_map = {}

    # 2. AI 매핑 안 된 컬럼은 규칙 기반으로 보완
    rule_map = detect_columns(df)
    for k, v in rule_map.items():
        if k not in col_map:
            col_map[k] = v

    # 3. 표준 컬럼명으로 통일
    rename = {v: k for k, v in col_map.items()}
    df = df.rename(columns=rename)

    if 'timestamp' in df.columns:
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df['hour']    = df['timestamp'].dt.hour
        df['date']    = df['timestamp'].dt.date
        df['weekday'] = df['timestamp'].dt.dayofweek

    if 'power_kw' in df.columns and df['power_kw'].mean() > 100:
        df['power_kw'] = df['power_kw'] / 1000

    return df, col_map

def detect_idle(df, col_map):
    results = []
    gpu_col = 'gpu_id' if 'gpu_id' in df.columns else None
    gpus = df[gpu_col].unique() if gpu_col else ['all']

    for gpu in gpus:
        gdf = df[df[gpu_col] == gpu] if gpu_col else df
        if 'gpu_util' not in gdf.columns:
            continue

        threshold = min(gdf['gpu_util'].quantile(0.20), 25)
        hourly_mean = gdf.groupby('hour')['gpu_util'].mean()
        hourly_std  = gdf.groupby('hour')['gpu_util'].std().fillna(5)

        idle_rows = []
        for _, row in gdf.iterrows():
            h = row['hour']
            z = (row['gpu_util'] - hourly_mean.get(h, 50)) / max(hourly_std.get(h, 10), 1)
            if z < -0.8 and row['gpu_util'] < 30:
                idle_rows.append(row)

        if not idle_rows:
            continue

        idle_df = pd.DataFrame(idle_rows)
        idle_hours = len(idle_df)
        rate = idle_df['electricity_rate'].mean() if 'electricity_rate' in idle_df.columns else 3.20
        power_savings = (idle_df['power_kw'].mean() * 0.65 * idle_hours * 0.12) if 'power_kw' in idle_df.columns else 0
        instance_savings = idle_hours * rate * 0.70
        total_savings = power_savings + instance_savings
        confidence = min(95, 60 + (idle_hours / 10) + abs(len(idle_df) / len(gdf) * 30))
        worst_hour = idle_df.groupby('hour').size().idxmax() if 'hour' in idle_df.columns else 0

        results.append({
            'gpu_id': gpu, 'idle_hours': idle_hours,
            'avg_util_pct': round(idle_df['gpu_util'].mean(), 1),
            'worst_hour': worst_hour,
            'monthly_savings': round(total_savings, 2),
            'confidence_pct': round(confidence, 0),
        })

    return pd.DataFrame(results).sort_values('monthly_savings', ascending=False) if results else pd.DataFrame()

def detect_peak_jobs(df, col_map):
    if 'electricity_rate' not in df.columns:
        return {'peak_hours_count': 0, 'current_cost': 0, 'monthly_savings': 0, 'offpeak_rate': 0}

    rate_p75 = df['electricity_rate'].quantile(0.75)
    rate_p25 = df['electricity_rate'].quantile(0.25)
    peak_mask = df['electricity_rate'] >= rate_p75
    offpeak_mask = df['electricity_rate'] <= rate_p25

    if 'workload_type' in df.columns:
        training_mask = df['workload_type'].str.lower().str.contains('train|batch', na=False)
        peak = df[peak_mask & training_mask]
    elif 'gpu_util' in df.columns:
        peak = df[peak_mask & (df['gpu_util'] > 70)]
    else:
        peak = df[peak_mask]

    offpeak_rate = df[offpeak_mask]['electricity_rate'].mean()
    if len(peak) == 0:
        return {'peak_hours_count': 0, 'current_cost': 0, 'monthly_savings': 0, 'offpeak_rate': round(offpeak_rate, 2)}

    current_cost = peak['electricity_rate'].sum()
    savings = (peak['electricity_rate'] - offpeak_rate).sum()
    return {
        'peak_hours_count': len(peak),
        'current_cost': round(current_cost, 2),
        'monthly_savings': round(max(savings, 0), 2),
        'offpeak_rate': round(offpeak_rate, 4),
        'peak_rate': round(df[peak_mask]['electricity_rate'].mean(), 4),
    }

def detect_overprovision(df, col_map):
    if 'gpu_id' not in df.columns:
        return {'total_gpus': 0, 'monthly_savings': 0, 'top_waste_hours': pd.DataFrame()}

    total_gpus = df['gpu_id'].nunique()
    rate = df['electricity_rate'].mean() if 'electricity_rate' in df.columns else 3.20

    hourly = df.groupby(['date', 'hour']).agg(
        gpus_on=('gpu_id', 'nunique')
    ).reset_index()
    by_hour = hourly.groupby('hour').agg(
        avg_on=('gpus_on', 'mean'),
        p95_on=('gpus_on', lambda x: x.quantile(0.95))
    ).reset_index()

    rows = []
    for _, row in by_hour.iterrows():
        needed = min(int(row['p95_on'] * 1.25) + 1, total_gpus)
        reducible = max(0, total_gpus - needed)
        if reducible >= 1:
            rows.append({
                'hour': int(row['hour']),
                'avg_on': round(row['avg_on'], 1),
                'p95_demand': round(row['p95_on'], 1),
                'reducible': reducible,
                'monthly_saving': round(reducible * rate * 30, 2),
            })

    savings_df = pd.DataFrame(rows)
    total = savings_df['monthly_saving'].sum() if len(savings_df) > 0 else 0
    top = savings_df.nlargest(5, 'monthly_saving') if len(savings_df) > 0 else pd.DataFrame()

    return {'total_gpus': total_gpus, 'monthly_savings': round(total, 2), 'top_waste_hours': top}

def detect_thermal(df, col_map):
    if 'temp_c' not in df.columns:
        return None
    results = []
    gpu_col = 'gpu_id' if 'gpu_id' in df.columns else None
    gpus = df[gpu_col].unique() if gpu_col else ['all']
    for gpu in gpus:
        gdf = df[df[gpu_col] == gpu] if gpu_col else df
        throttle = (gdf['temp_c'] > 85).mean() * 100
        results.append({
            'gpu_id': gpu,
            'avg_temp_c': round(gdf['temp_c'].mean(), 1),
            'max_temp_c': round(gdf['temp_c'].max(), 1),
            'throttle_risk_pct': round(throttle, 1),
        })
    return pd.DataFrame(results)

def run_analysis(filepath='gpu_metrics_30d.csv'):
    print("=" * 62)
    print("  InfraLens — AI Infrastructure Cost Analysis v3.0")
    print("=" * 62)

    df, col_map = load_data(filepath, chunksize=100000)
    tier = 'Pro' if len(col_map) >= 7 else ('Standard' if len(col_map) >= 4 else 'Basic')

    print(f"\n  Data:  {len(df):,} rows | {df['gpu_id'].nunique() if 'gpu_id' in df.columns else '?'} devices | {df['date'].nunique() if 'date' in df.columns else '?'} days")
    print(f"  Tier:  {tier} ({len(col_map)} columns mapped)")
    print(f"  AI mapped: {list(col_map.keys())}\n")

    idle    = detect_idle(df, col_map)
    peak    = detect_peak_jobs(df, col_map)
    over    = detect_overprovision(df, col_map)
    thermal = detect_thermal(df, col_map)

    idle_total = idle['monthly_savings'].sum() if len(idle) > 0 else 0
    total = idle_total + peak['monthly_savings'] + over['monthly_savings']

    print("[ FINDING 01 ]  Idle Waste")
    print("-" * 62)
    if len(idle) > 0:
        for _, row in idle.iterrows():
            print(f"  {row['gpu_id']}  |  {row['avg_util_pct']}% avg  |  {row['idle_hours']}h  |  worst {row['worst_hour']:02d}:00  |  ${row['monthly_savings']:,.0f}/mo  |  {row['confidence_pct']:.0f}% confidence")
        print(f"\n  → Total: ${idle_total:,.2f}/month\n")
    else:
        print("  No significant idle waste.\n")

    print("[ FINDING 02 ]  Peak-Rate Scheduling")
    print("-" * 62)
    if peak['peak_hours_count'] > 0:
        print(f"  Peak sessions:  {peak['peak_hours_count']}")
        print(f"  Current cost:   ${peak['current_cost']:,.4f}/mo")
        print(f"  Off-peak rate:  ${peak['offpeak_rate']}/hr")
        print(f"  Savings:        ${peak['monthly_savings']:,.2f}/mo\n")
    else:
        print("  No peak waste detected.\n")

    print("[ FINDING 03 ]  Overprovisioning")
    print("-" * 62)
    if over['total_gpus'] > 0 and len(over['top_waste_hours']) > 0:
        for _, row in over['top_waste_hours'].iterrows():
            print(f"  {int(row['hour']):02d}:00  |  avg {row['avg_on']} on  |  reducible {row['reducible']}  |  ${row['monthly_saving']:,.0f}/mo")
        print(f"\n  → Total: ${over['monthly_savings']:,.2f}/month\n")
    else:
        print("  No overprovisioning detected.\n")

    if thermal is not None and len(thermal) > 0:
        print("[ FINDING 04 ]  Thermal Analysis")
        print("-" * 62)
        for _, row in thermal.iterrows():
            flag = ' ⚠ RISK' if row['throttle_risk_pct'] > 5 else ''
            print(f"  {row['gpu_id']}  |  avg {row['avg_temp_c']}°C  |  max {row['max_temp_c']}°C{flag}")
        print()

    print("=" * 62)
    print(f"  TOTAL SAVINGS    ${total:>10,.2f} / month")
    print(f"  ANNUAL           ${total*12:>10,.2f} / year")
    print("=" * 62)

if __name__ == '__main__':
    import sys
    filepath = sys.argv[1] if len(sys.argv) > 1 else 'gpu_metrics_30d.csv'
    run_analysis(filepath)
