# InfraLens ⚡
### AI-Powered GPU & Infrastructure Cost Optimization

> Upload any server/GPU metrics CSV → AI auto-detects columns → Get **exact dollar savings** with time-specific action plans

[![Python](https://img.shields.io/badge/Python-3.10-blue)](https://python.org)
[![Streamlit](https://img.shields.io/badge/Streamlit-1.x-red)](https://streamlit.io)
[![Claude AI](https://img.shields.io/badge/Claude-AI%20Powered-purple)](https://anthropic.com)
[![Live Demo](https://img.shields.io/badge/Live-Demo-brightgreen)](https://infralens.streamlit.app)

**[▶ Live Demo](https://infralens.streamlit.app)** — Upload your CSV or try sample data

[한국어 버전 보기 →](#한국어)

---

## The Problem

AI companies are burning money on GPU infrastructure — and most don't know exactly where.

- Average GPU utilization at AI startups: **30–40%** — the rest is idle spend
- **30–35%** of all cloud spend is wasted (Gartner 2026)
- **98%** of FinOps teams now manage AI costs (State of FinOps 2026)
- Existing tools give vague advice: *"reduce your PUE"* — not actionable

**InfraLens gives you:**
*"GPU-03 is idle 02:00–06:00 daily → enable power-saving → save $1,302/month"*

---

## What It Does

```
Upload CSV → AI detects columns → Advanced analysis → Exact savings + action plan
```

### Real Output Example

```
EXECUTIVE SUMMARY
  Current monthly cost:    $27,573
  After optimization:      $16,624
  Monthly savings:         $10,948  (39.7%)
  Annual savings:          $131,381
  Payback period:          Immediate

ACTION PLAN — sorted by impact

  [1] Reschedule 1,506 peak-hour training jobs
      Training at $3.65/hr during peak. Off-peak: $2.10/hr (42% cheaper)
      Action: Move batch jobs to 02:00-06:00 using cron/Slurm scheduler
      Save $2,335/month | Effort: Low | Timeframe: This week | Confidence: 85%

  [2] Enable power-saving on 8 idle GPUs
      8 GPUs at 10% avg utilization during off-hours, drawing full power
      Action: nvidia-smi --auto-boost-default=0, set persistence mode
      Save $1,302/month | Effort: Low | Timeframe: Immediate | Confidence: 80%

  [3] Scale down GPU fleet overnight
      Only 1 GPU needed at 02:00 (p95). 3 can be safely removed.
      Action: Kubernetes HPA or custom scaling, 20% buffer maintained
      Save $84/month | Effort: Medium | Confidence: 75%
```

---

## Key Features

| Feature | Description |
|---------|-------------|
| AI Column Mapping | Claude AI auto-detects any CSV format, no manual setup |
| Rolling Average Detection | 3h/24h rolling stats + Z-score anomaly detection |
| TOU Cost Modeling | Time-of-use pricing (AWS, GCP, KEPCO) + cooling overhead |
| Before/After Simulation | Exact monthly/annual savings with cooling included |
| GPU Efficiency Grades | A-D scoring per device with waste percentage |
| Priority Action Plans | Sorted by impact with effort, timeframe, confidence |
| Full Report Download | Exportable .txt report for stakeholders |

---

## Architecture

```
infralens/
├── data_loader.py      # AI column mapping + data cleaning + rolling stats
├── cost_model.py       # TOU pricing + cooling overhead + before/after simulation
├── analyzer.py         # Z-score idle detection + peak analysis + efficiency scores
├── recommender.py      # Priority ranking + actionable recommendations
├── app.py              # Streamlit UI
├── generate_sample.py  # Synthetic data generator
└── requirements.txt
```

### Algorithm Overview

**1. Idle Detection** — Rolling average + Z-score

```python
# Old approach
if gpu_util < 10%: idle

# InfraLens v4.0
rolling_3h_avg < 25%
AND z_score < -0.5   # below hourly baseline
AND current_util < 35%
# Higher precision, fewer false positives
```

**2. Cost Modeling** — Time-of-use pricing with cooling overhead

```python
savings = sum((peak_rate - offpeak_rate) x movable_hours)
        + idle_power_kw x 0.70 x idle_hours x rate
        + cooling_overhead_reduction
```

**3. Efficiency Score** — Multi-factor 0 to 100

```python
score = util_score       x 0.4
      + consistency_score x 0.3
      + waste_score        x 0.3
```

---

## Supported CSV Formats

Supports any format — AI auto-detects column meanings.

| Standard Field | Recognized Column Names |
|---------------|------------------------|
| timestamp | timestamp, time, datetime, ts, date |
| gpu_id | gpu_id, device_id, server_id, gpu |
| gpu_util | gpu_util, utilization, Server_Workload(%), cpu_util |
| power_kw | power_kw, power_watt, watts, power_consumption |
| electricity_rate | electricity_rate, cost_per_hr, Total_Energy_Cost($) |
| temp_c | temp_c, temperature, Inlet_Temperature |
| cooling_kw | cooling_kw, Cooling_Unit_Power_Consumption(kW) |

**Tiers:** Basic (4+ cols) → Standard (6+ cols) → Pro (8+ cols)

---

## Quick Start

```bash
git clone https://github.com/SamJeong7201/Infralens
cd Infralens
pip install -r requirements.txt
echo "ANTHROPIC_API_KEY=your_key_here" > .env
streamlit run app.py
```

---

## Tech Stack

| Layer | Tech |
|-------|------|
| Language | Python 3.10 |
| Data | Pandas, NumPy, SciPy |
| AI Mapping | Anthropic Claude API |
| Frontend | Streamlit + Plotly |
| Deployment | Streamlit Cloud |

---

## Market Context

| Metric | Data | Source |
|--------|------|--------|
| AI-optimized IaaS market 2026 | $37.5B | Gartner |
| Cloud budget wasted | 30-35% | Flexera 2026 |
| FinOps teams managing AI costs | 98% | State of FinOps 2026 |
| Potential savings via FinOps | $21B | Deloitte 2025 |

---

## Roadmap

- [x] v1.0 — Rule-based idle detection + Streamlit MVP
- [x] v2.0 — AI column mapping + dynamic thresholds
- [x] v3.0 — Modular architecture + TOU cost model
- [x] v4.0 — Rolling average + Z-score + before/after simulation
- [ ] v5.0 — ML anomaly detection (Isolation Forest)
- [ ] v6.0 — Linear programming workload scheduler
- [ ] v7.0 — Real-time monitoring (Prometheus / NVIDIA DCGM)

---

## License

MIT — use it, fork it, build on it.

---

*Built to make AI infrastructure cost optimization concrete —
not a dashboard metric, but an exact dollar-and-time action plan.*

---
---

# 한국어

# InfraLens ⚡
### AI 기반 GPU & 인프라 비용 최적화 도구

> 어떤 서버/GPU 메트릭 CSV든 업로드하면 → AI가 컬럼을 자동 인식 → **정확한 절감액과 시간별 액션 플랜** 제공

**[▶ 라이브 데모](https://infralens.streamlit.app)** — CSV를 업로드하거나 샘플 데이터로 체험해보세요

---

## 문제

AI 기업들은 GPU 인프라 비용을 낭비하고 있습니다 — 그리고 대부분은 정확히 어디서 낭비되는지 모릅니다.

- AI 스타트업의 평균 GPU 사용률: **30–40%** — 나머지는 idle 낭비
- 전체 클라우드 지출의 **30–35%** 가 낭비됨 (Gartner 2026)
- FinOps 팀의 **98%** 가 AI 비용을 관리 중 (State of FinOps 2026)
- 기존 도구들은 추상적인 조언만 제공: *"PUE를 개선하세요"* — 실행 불가능

**InfraLens는 이렇게 알려줍니다:**
*"GPU-03이 매일 02:00–06:00에 idle 상태입니다 → 절전 모드 활성화 → 월 $1,302 절감"*

---

## 작동 방식

```
CSV 업로드 → AI 컬럼 자동 인식 → 고급 분석 → 정확한 절감액 + 액션 플랜
```

### 실제 출력 예시

```
요약
  현재 월 비용:      $27,573
  최적화 후 비용:    $16,624
  월 절감액:         $10,948  (39.7%)
  연간 절감액:       $131,381
  회수 기간:         즉시

액션 플랜 — 임팩트 순 정렬

  [1] 피크 시간대 학습 작업 1,506건 재스케줄링
      현재 피크 요금 $3.65/hr → 오프피크 $2.10/hr (42% 저렴)
      실행: cron/Slurm으로 02:00-06:00 배치 학습 예약
      절감: $2,335/월 | 난이도: 낮음 | 기간: 이번 주 | 신뢰도: 85%

  [2] Idle 상태 GPU 8대 절전 모드 활성화
      8대 GPU가 off-hours에 10% 평균 사용률로 풀 파워 소비 중
      실행: nvidia-smi --auto-boost-default=0, persistence mode 설정
      절감: $1,302/월 | 난이도: 낮음 | 기간: 즉시 | 신뢰도: 80%

  [3] 야간 GPU 플릿 축소
      02:00 기준 1대만 필요 (p95). 3대 안전하게 절감 가능
      실행: Kubernetes HPA 또는 커스텀 스케일링 (20% 버퍼 유지)
      절감: $84/월 | 난이도: 중간 | 신뢰도: 75%
```

---

## 주요 기능

| 기능 | 설명 |
|------|------|
| AI 컬럼 자동 매핑 | Claude AI가 어떤 CSV 형식이든 자동 인식 |
| 롤링 평균 탐지 | 3h/24h 롤링 통계 + Z-score 이상 탐지 |
| TOU 비용 모델 | 시간대별 요금 (AWS, GCP, 한전) + 냉각 오버헤드 |
| Before/After 시뮬레이션 | 냉각 포함 정확한 월/연간 절감액 계산 |
| GPU 효율 등급 | 기기별 A~D 등급 + 낭비율 |
| 우선순위 액션 플랜 | 임팩트 순 정렬 + 난이도/기간/신뢰도 |
| 리포트 다운로드 | 이해관계자용 .txt 리포트 내보내기 |

---

## 아키텍처

```
infralens/
├── data_loader.py      # AI 컬럼 매핑 + 데이터 정제 + 롤링 통계
├── cost_model.py       # TOU 요금 + 냉각 오버헤드 + before/after 시뮬레이션
├── analyzer.py         # Z-score idle 탐지 + 피크 분석 + 효율 점수
├── recommender.py      # 우선순위 정렬 + 구체적 액션 플랜 생성
├── app.py              # Streamlit UI
├── generate_sample.py  # 합성 데이터 생성기
└── requirements.txt
```

---

## 지원 CSV 형식

어떤 형식이든 지원 — AI가 컬럼 의미를 자동으로 파악합니다.

| 표준 필드 | 인식 가능한 컬럼명 |
|----------|-----------------|
| timestamp | timestamp, time, datetime, ts, date |
| gpu_id | gpu_id, device_id, server_id, gpu |
| gpu_util | gpu_util, utilization, Server_Workload(%), cpu_util |
| power_kw | power_kw, power_watt, watts, power_consumption |
| electricity_rate | electricity_rate, cost_per_hr, Total_Energy_Cost($) |
| temp_c | temp_c, temperature, Inlet_Temperature |

**분석 티어:** Basic (4+ 컬럼) → Standard (6+ 컬럼) → Pro (8+ 컬럼)

---

## 빠른 시작

```bash
git clone https://github.com/SamJeong7201/Infralens
cd Infralens
pip install -r requirements.txt
echo "ANTHROPIC_API_KEY=your_key_here" > .env
streamlit run app.py
```

---

## 시장 규모

| 지표 | 수치 | 출처 |
|------|------|------|
| AI 최적화 IaaS 시장 (2026) | $37.5B | Gartner |
| 클라우드 예산 낭비율 | 30-35% | Flexera 2026 |
| FinOps팀 AI 비용 관리 비율 | 98% | State of FinOps 2026 |
| FinOps로 절감 가능 금액 | $21B | Deloitte 2025 |

---

## 로드맵

- [x] v1.0 — 규칙 기반 idle 탐지 + Streamlit MVP
- [x] v2.0 — AI 컬럼 자동 매핑 + 동적 임계값
- [x] v3.0 — 모듈 분리 + TOU 비용 모델
- [x] v4.0 — 롤링 평균 + Z-score + before/after 시뮬레이션
- [ ] v5.0 — ML 이상 탐지 (Isolation Forest)
- [ ] v6.0 — 선형 프로그래밍 워크로드 스케줄러
- [ ] v7.0 — 실시간 모니터링 (Prometheus / NVIDIA DCGM)

---

*AI 인프라 비용 최적화를 구체적이고 실행 가능하게 만들기 위해 제작됐습니다 —
대시보드 숫자가 아니라, 정확한 달러와 시간이 담긴 액션 플랜.*
