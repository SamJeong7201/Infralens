# InfraLens ⚡
### Local-First GPU Infrastructure Optimization Agent

> Runs **inside your server** — detects problems, auto-fixes, sends reports. Your data never leaves.

[![Python](https://img.shields.io/badge/Python-3.10-blue)](https://python.org)
[![Streamlit](https://img.shields.io/badge/Streamlit-1.x-red)](https://streamlit.io)
[![License](https://img.shields.io/badge/License-MIT-green)](LICENSE)

**[한국어 버전 보기 →](#한국어)****

---

## The Problem

GPU servers waste money silently.

- Average GPU utilization at AI teams: **30–40%** — the rest is idle
- **30–35%** of all cloud spend is wasted (Flexera 2026)
- Existing tools require cloud access — **your data leaves your server**
- Most tools just show dashboards — they don't **fix** anything

**InfraLens runs inside your server and tells you:**
*"GPU-5 has a zombie process occupying 23.4GB VRAM for 6h → here's the kill command → approve?"*

---

## How It Works

```
Install on your server (1 line)
        ↓
Agent collects nvidia-smi data locally
        ↓
7 detection algorithms find problems
        ↓
Auto-fixes safe issues, flags manual ones
        ↓
Dashboard for approval flow
        ↓
Weekly PDF report → Manager/CFO email
        ↓
Alert email → Admin (critical issues only)
```

**Your data never leaves the server.**

---

## What It Detects

| Problem | Algorithm | Auto-Fix |
|---------|-----------|----------|
| Idle GPU (power waste) | EMA + time-aware threshold | ✅ Power limit |
| Memory leak | Linear regression slope | ⚠️ Manual |
| Zombie process | util/VRAM ratio anomaly | ⚠️ Manual |
| GPU imbalance | Gini coefficient | ⚠️ Manual |
| Overheating | Threshold + PES score | ⚠️ Manual |
| Low power efficiency | Power Efficiency Score | ✅ Power limit |
| Overprovisioning | Peak-hour average | ✅ Power limit |

---

## Real Output Example

```
==================================================
InfraLens Agent — 2026-05-02 23:05
==================================================
GPUs:         8
Overall util: 34.1%
Total power:  1269.1W
Alerts:       12 (0 critical, 5 high, 7 medium)

⚠️  ALERTS:
  [HIGH] GPU-4 possible memory leak: VRAM growing +800MB/h
         at 10% util (73% full, OOM in ~11h)
  [HIGH] GPU-5 zombie process: 1% util but 75% VRAM occupied
         (23.4GB blocked for 23h)
  [HIGH] GPU imbalance (Gini=0.58): 2 GPUs overloaded,
         5 GPUs underused at 34% avg

📊 vs last run:
  📈 Util:   +1.2%
  💚 Power:  -45W
  🔻 Alerts: -2

🤖 AUTO EXECUTE — 5 safe actions
   → IDLE_GPU: nvidia-smi -i 2 -pl 75  ✅
   → IDLE_GPU: nvidia-smi -i 3 -pl 75  ✅
   → LOW_POWER_EFFICIENCY: nvidia-smi -i 6 -pl 120  ✅
```

---

## Two Products

### 1. InfraLens Agent (Local)
Runs on your GPU server. Collects, analyzes, auto-fixes.

```bash
# Install (1 line)
curl -sSL https://raw.githubusercontent.com/SamJeong7201/Infralens/main/infralens_agent/install.sh | bash

# Run once
cd ~/infralens-agent && python3 run.py

# Run with auto-fix
python3 run.py --auto

# Run continuously (every 5 min)
python3 run.py --loop --auto

# Open approval dashboard
streamlit run dashboard.py
```

### 2. InfraLens Web (CSV Upload)
Upload any GPU metrics CSV → get analysis + PDF report.

```bash
git clone https://github.com/SamJeong7201/Infralens
cd Infralens
pip install -r requirements.txt
streamlit run app.py
```


---

## Agent Architecture

```
infralens_agent/
├── collect.py          # nvidia-smi → local SQLite (no external transfer)
├── analyze/
│   ├── anomaly.py      # Z-score, IQR, EMA, linear regression, Gini
│   ├── idle.py         # EMA + time-aware idle detection
│   ├── memory.py       # Linear regression memory leak
│   ├── zombie.py       # util/VRAM ratio anomaly
│   ├── balance.py      # Gini coefficient imbalance
│   └── power.py        # PES score, thermal, overprovisioning
├── tracker.py          # Snapshot + change detection + recurring issues
├── env_detect.py       # AWS / GCP / Azure / Slurm auto-detection
├── execute.py          # Approved command execution + rollback
├── dashboard.py        # Approval flow UI (Streamlit)
├── run.py              # Main loop (collect → analyze → auto-fix → notify)
├── notify/
│   └── mailer.py       # Admin alert + Manager weekly report (summary only)
└── install.sh          # One-line installer
```

---

## Detection Algorithms

```python
# Idle Detection — EMA + time-aware threshold
util_ema = exponential_moving_average(gpu_util, span=6)
expected = 20% if business_hours else 5%  # time-aware
if util_ema < expected for 30+ min → IDLE alert

# Memory Leak — Linear Regression
slope = linear_regression(mem_used_mb over time)
if slope > 50 MB/h and gpu_util < 30% → LEAK alert
hours_to_oom = remaining_mb / slope  # OOM prediction

# GPU Imbalance — Gini Coefficient
gini = gini_coefficient([gpu_util for all GPUs])
if gini > 0.4 → WARNING
if gini > 0.6 → CRITICAL

# Power Efficiency Score
PES = (gpu_util / 100) / (power_draw / power_limit)
if PES < 0.3 and util < 20% → LOW_EFFICIENCY alert
```

---

## Security

```
✅ All data stays on your server
✅ Local SQLite only — no cloud database
✅ Only summaries sent via email (no raw data)
✅ Execution disabled by default (dry_run: true)
✅ Every command has a rollback
✅ Works on air-gapped servers
```

---

## Supported Environments

| Environment | Status |
|-------------|--------|
| On-premise Linux | ✅ |
| AWS (p2, p3, p4 instances) | ✅ Auto-detected |
| GCP (A100, V100 instances) | ✅ Auto-detected |
| Azure (NC series) | ✅ Auto-detected |
| Slurm clusters | ✅ Auto-detected |
| Multi-GPU (1–100+ GPUs) | ✅ |

---

## Notification Flow

```
Problem detected
      ↓
[CRITICAL/HIGH] → Admin email immediately
      ↓
Every Monday 9AM → Manager PDF report email

Admin email:     problem table + commands (no raw data)
Manager email:   cost savings + trend (summary only)
```

---

## Market Context

| Metric | Data | Source |
|--------|------|--------|
| AI IaaS market 2026 | $37.5B | Gartner |
| Cloud budget wasted | 30-35% | Flexera 2026 |
| FinOps teams managing AI costs | 98% | State of FinOps 2026 |
| Potential savings via optimization | $21B | Deloitte 2025 |

---

## Roadmap

- [x] v1.0 — CSV upload + AI analysis + PDF report
- [x] v2.0 — Local agent + nvidia-smi collection
- [x] v3.0 — 7 detection algorithms (EMA, regression, Gini)
- [x] v4.0 — Auto-fix + approval dashboard + change tracking
- [x] v5.0 — Environment detection + email notifications
- [ ] v6.0 — Multi-server management
- [ ] v7.0 — ML-based anomaly detection (Isolation Forest)
- [ ] v8.0 — Workload scheduler (peak → off-peak automation)
- [ ] v9.0 — Cloud cost comparison (on-prem vs AWS/GCP)

---

## License

MIT — use it, fork it, build on it.

---

*Built for AI teams who want GPU cost optimization that's local-first, actionable, and automatic —
not another dashboard, but an agent that actually fixes things.*

---
---

# 한국어

# InfraLens ⚡
### 로컬 퍼스트 GPU 인프라 최적화 에이전트

> **서버 안에서** 직접 실행 — 문제 감지, 자동 수정, 보고서 발송. 데이터는 절대 외부로 나가지 않습니다.


---

## 문제

GPU 서버는 조용히 돈을 낭비합니다.

- AI 팀의 평균 GPU 사용률: **30–40%** — 나머지는 idle 낭비
- 전체 클라우드 지출의 **30–35%** 가 낭비됨 (Flexera 2026)
- 기존 도구들은 클라우드 접근 필요 — **데이터가 서버 밖으로 나감**
- 대부분 도구는 대시보드만 보여줌 — 실제로 **고치지 않음**

**InfraLens는 서버 안에서 직접 알려줍니다:**
*"GPU-5에 좀비 프로세스가 23.4GB VRAM을 6시간째 점유 중 → 종료 명령어 → 승인?"*

---

## 작동 방식

```
서버에 설치 (1줄)
        ↓
에이전트가 nvidia-smi 데이터 로컬 수집
        ↓
7가지 감지 알고리즘으로 문제 탐지
        ↓
안전한 문제 자동 수정, 위험한 문제 플래그
        ↓
대시보드에서 승인 플로우
        ↓
주간 PDF 보고서 → 매니저/CFO 이메일
        ↓
즉시 알림 → 관리자 이메일 (critical만)
```

**데이터는 절대 서버 밖으로 나가지 않습니다.**

---

## 감지 항목

| 문제 | 알고리즘 | 자동 수정 |
|------|----------|----------|
| Idle GPU (전력 낭비) | EMA + 시간대별 임계값 | ✅ 전력 제한 |
| 메모리 누수 | 선형회귀 기울기 | ⚠️ 수동 |
| 좀비 프로세스 | util/VRAM 비율 이상감지 | ⚠️ 수동 |
| GPU 불균형 | 지니계수 | ⚠️ 수동 |
| 과열 | 임계값 + PES 점수 | ⚠️ 수동 |
| 전력 효율 저하 | Power Efficiency Score | ✅ 전력 제한 |
| 오버프로비저닝 | 피크타임 평균 | ✅ 전력 제한 |

---

## 실제 출력 예시

```
==================================================
InfraLens Agent — 2026-05-02 23:05
==================================================
GPUs:         8
Overall util: 34.1%
Total power:  1269.1W
Alerts:       12 (0 critical, 5 high, 7 medium)

⚠️  ALERTS:
  [HIGH] GPU-4 메모리 누수: VRAM +800MB/h 증가
         사용률 10% (73% 충전, ~11시간 후 OOM)
  [HIGH] GPU-5 좀비 프로세스: 사용률 1%이지만
         VRAM 75% 점유 (23.4GB, 23시간째)
  [HIGH] GPU 불균형 (Gini=0.58): 2개 과부하,
         5개 저사용 (평균 34%)

📊 이전 대비 변화:
  📈 Util:   +1.2%
  💚 Power:  -45W
  🔻 Alerts: -2

🤖 자동 실행 — 5개 안전한 액션
   → IDLE_GPU: nvidia-smi -i 2 -pl 75  ✅
   → IDLE_GPU: nvidia-smi -i 3 -pl 75  ✅
   → LOW_POWER_EFFICIENCY: nvidia-smi -i 6 -pl 120  ✅
```

---

## 두 가지 제품

### 1. InfraLens Agent (로컬)
GPU 서버에서 직접 실행. 수집, 분석, 자동 수정.

```bash
# 설치 (1줄)
curl -sSL https://raw.githubusercontent.com/SamJeong7201/Infralens/main/infralens_agent/install.sh | bash

# 한 번 실행
cd ~/infralens-agent && python3 run.py

# 자동 수정 포함 실행
python3 run.py --auto

# 5분마다 자동 반복
python3 run.py --loop --auto

# 승인 대시보드 열기
streamlit run dashboard.py
```

### 2. InfraLens Web (CSV 업로드)
GPU 메트릭 CSV 업로드 → 분석 + PDF 보고서.


---

## 보안

```
✅ 모든 데이터가 서버 안에만 있음
✅ 로컬 SQLite만 사용 — 클라우드 DB 없음
✅ 이메일엔 요약본만 전송 (raw 데이터 없음)
✅ 실행 기본 비활성화 (dry_run: true)
✅ 모든 명령어에 rollback 있음
✅ air-gapped 서버에서도 작동
```

---

## 지원 환경

| 환경 | 상태 |
|------|------|
| On-premise Linux | ✅ |
| AWS (p2, p3, p4 인스턴스) | ✅ 자동 감지 |
| GCP (A100, V100 인스턴스) | ✅ 자동 감지 |
| Azure (NC 시리즈) | ✅ 자동 감지 |
| Slurm 클러스터 | ✅ 자동 감지 |
| 멀티 GPU (1–100+ GPUs) | ✅ |

---

## 알림 플로우

```
문제 감지
      ↓
[CRITICAL/HIGH] → 관리자 즉시 이메일
      ↓
매주 월요일 오전 9시 → 매니저 PDF 보고서 이메일

관리자 이메일: 문제 표 + 명령어 (raw 데이터 없음)
매니저 이메일: 비용 절감 + 트렌드 (요약본만)
```

---

## 로드맵

- [x] v1.0 — CSV 업로드 + AI 분석 + PDF 보고서
- [x] v2.0 — 로컬 에이전트 + nvidia-smi 수집
- [x] v3.0 — 7가지 감지 알고리즘 (EMA, 선형회귀, 지니계수)
- [x] v4.0 — 자동 수정 + 승인 대시보드 + 변화 추적
- [x] v5.0 — 환경 감지 + 이메일 알림
- [ ] v6.0 — 멀티 서버 관리
- [ ] v7.0 — ML 기반 이상감지 (Isolation Forest)
- [ ] v8.0 — 워크로드 스케줄러 (피크 → 오프피크 자동화)
- [ ] v9.0 — 클라우드 비용 비교 (on-prem vs AWS/GCP)

---

## 라이선스

MIT — 사용, 포크, 빌드 자유롭게.

---

*데이터가 서버 밖으로 나가지 않으면서 GPU 비용을 자동으로 최적화하는 에이전트 —
대시보드가 아니라 실제로 고쳐주는 툴.*
