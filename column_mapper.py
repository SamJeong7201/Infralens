import os
import json
from anthropic import Anthropic
from dotenv import load_dotenv

load_dotenv()
client = Anthropic()

def auto_map_columns(df_columns: list) -> dict:
    """
    AI가 CSV 컬럼을 자동으로 분석해서 표준 컬럼으로 매핑
    """
    prompt = f"""You are an expert in data center and GPU infrastructure data.

I have a CSV file with these column names:
{json.dumps(df_columns, indent=2)}

Map each column to one of these standard fields (or null if not relevant):
- timestamp: date/time of the measurement
- gpu_id: GPU or server identifier
- gpu_util: GPU or CPU utilization percentage (0-100)
- memory_util: memory utilization percentage (0-100)
- power_kw: power consumption in kW or W
- temp_c: temperature in Celsius
- cooling_kw: cooling power consumption
- electricity_rate: electricity cost or rate
- workload_type: type of workload (training/inference/idle)

Respond with ONLY a valid JSON object like this:
{{
  "timestamp": "Timestamp",
  "gpu_util": "Server_Workload(%)",
  "power_kw": "Cooling_Unit_Power_Consumption(kW)",
  "electricity_rate": "Total_Energy_Cost($)",
  "temp_c": "Inlet_Temperature(°C)",
  "cooling_kw": null,
  "gpu_id": null,
  "memory_util": null,
  "workload_type": null
}}

Only include fields where you found a match. Use null for unmatched fields."""

    message = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=500,
        messages=[{"role": "user", "content": prompt}]
    )

    response_text = message.content[0].text.strip()

    # JSON만 추출
    if '```' in response_text:
        response_text = response_text.split('```')[1]
        if response_text.startswith('json'):
            response_text = response_text[4:]

    mapping = json.loads(response_text)
    return {k: v for k, v in mapping.items() if v is not None}

def test_mapping():
    import pandas as pd

    # 실제 Kaggle 데이터로 테스트
    df = pd.read_csv('cold_source_control_dataset.csv')
    columns = df.columns.tolist()

    print("원본 컬럼:")
    for c in columns:
        print(f"  {c}")

    print("\nAI 매핑 중...")
    mapping = auto_map_columns(columns)

    print("\nAI 매핑 결과:")
    for standard, original in mapping.items():
        print(f"  {standard:20s} ← {original}")

    # 실제로 컬럼 이름 바꿔서 분석 가능한지 확인
    reverse_map = {v: k for k, v in mapping.items()}
    df_mapped = df.rename(columns=reverse_map)

    print(f"\n매핑된 컬럼: {list(df_mapped.columns)}")
    print("\n분석 가능한 컬럼:")
    standard_cols = ['timestamp', 'gpu_util', 'power_kw', 'electricity_rate', 'temp_c']
    for col in standard_cols:
        if col in df_mapped.columns:
            print(f"  ✓ {col}")
        else:
            print(f"  ✗ {col} (없음)")

if __name__ == '__main__':
    test_mapping()
