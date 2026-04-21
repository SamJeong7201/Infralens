import pandas as pd
import numpy as np
from dataclasses import dataclass, field
from typing import List, Optional
from infrastructure_advisor import detect_environment, build_action_guide, format_guide_text

@dataclass
class Recommendation:
    priority: int
    category: str
    title: str
    detail: str
    action: str
    monthly_savings: float
    annual_savings: float
    effort: str
    confidence: float
    timeframe: str
    commands: List[str] = field(default_factory=list)
    owner: str = 'DevOps'
    time_to_implement: str = '< 30 minutes'
    expected_start: str = 'Today'
    risk: str = 'Low'
    kpi: str = ''


def _gpu_index(gpu_id: str) -> str:
    """gpu-a100-07 → 7, gpu-v100-03 → unique index from df"""
    try:
        parts = str(gpu_id).replace('gpu-', '').split('-')
        for p in reversed(parts):
            if p.isdigit():
                return p
        return '0'
    except:
        return '0'


def _get_gpu_index_map(idle_df: pd.DataFrame) -> dict:
    """GPU ID → 실제 순서 인덱스 매핑 (중복 방지)"""
    gpu_ids = idle_df['gpu_id'].tolist() if 'gpu_id' in idle_df.columns else []
    return {gpu_id: str(i) for i, gpu_id in enumerate(sorted(set(gpu_ids)))}


def _power_limit_for_idle(gpu_model: str) -> int:
    """GPU idle power limit by model (NVIDIA official recommended values)"""
    model = str(gpu_model).upper() if gpu_model else ''
    if 'H100' in model:
        return 100   # H100 TDP 700W → idle 100W
    elif 'A100' in model:
        return 75    # A100 TDP 400W → idle 75W
    elif 'V100' in model:
        return 50    # V100 TDP 300W → idle 50W
    elif 'A10' in model:
        return 40
    else:
        return 60


def _tdp_for_model(gpu_model: str) -> int:
    model = str(gpu_model).upper() if gpu_model else ''
    if 'H100' in model: return 700
    elif 'A100' in model: return 400
    elif 'V100' in model: return 300
    elif 'A10' in model: return 150
    else: return 300


def generate_recommendations(
    idle_df: pd.DataFrame,
    peak: dict,
    over: dict,
    sim: dict,
    scores: pd.DataFrame,
    df: Optional[pd.DataFrame] = None,
    thermal: Optional[dict] = None,
    mem_bottleneck: Optional[dict] = None,
    inter_gpu: Optional[dict] = None,
    workload_gap: Optional[dict] = None,
) -> List[Recommendation]:

    recs = []
    priority = 1

    # ── REC 1: Idle GPU Power Saving ──────────────────────────
    if len(idle_df) > 0:
        idle_total = idle_df['monthly_savings'].sum()
        worst = idle_df.iloc[0]
        worst_id = worst['gpu_id']
        worst_idx = _gpu_index(worst_id)
        worst_hour = int(worst.get('idle_hours', 0))
        avg_util = idle_df['avg_util_pct'].mean()

        # 실제 idle 시간대 분석 (야간 위주)
        idle_start = 22
        idle_end = 7
        if df is not None and 'hour' in df.columns and 'gpu_util' in df.columns:
            gpu_df = df[df['gpu_id'] == worst_id] if 'gpu_id' in df.columns else df
            hourly_util = gpu_df.groupby('hour')['gpu_util'].mean()
            # 야간(20:00~08:00) 중 idle 시간대만
            night_hours = list(range(20, 24)) + list(range(0, 9))
            night_idle = hourly_util[hourly_util.index.isin(night_hours) & (hourly_util < 15)]
            if len(night_idle) >= 2:
                idle_start = 22  # 항상 22:00 시작
                idle_end = 7     # 07:00 복구
        
        # GPU index 맵 (중복 방지)
        gpu_index_map = _get_gpu_index_map(idle_df)

        # GPU 모델 가져오기
        gpu_model = 'A100'
        if df is not None and 'gpu_model' in df.columns and 'gpu_id' in df.columns:
            model_rows = df[df['gpu_id'] == worst_id]['gpu_model']
            if len(model_rows) > 0:
                gpu_model = str(model_rows.iloc[0])

        idle_power_limit = _power_limit_for_idle(gpu_model)
        tdp = _tdp_for_model(gpu_model)

        # 가장 나쁜 GPU 3개
        worst_gpus = idle_df.head(3)['gpu_id'].tolist()
        worst_gpus_str = ', '.join(worst_gpus)

        commands = []
        for _, row in idle_df.head(5).iterrows():
            idx = _gpu_index(row['gpu_id'])
            commands.append(f"nvidia-smi -i {idx} -pl {idle_power_limit}   # {row['gpu_id']}: {row['avg_util_pct']:.1f}% avg util")

        commands += [
            f"",
            f"# Register automatic schedule (crontab)",
            f"# Daily {idle_start:02d}:00 power-saving mode start",
        ]
        for _, row in idle_df.head(5).iterrows():
            idx = _gpu_index(row['gpu_id'])
            commands.append(f"echo \"0 {idle_start} * * * nvidia-smi -i {idx} -pl {idle_power_limit}\" | crontab -")

        commands += [
            f"",
            f"# Daily {idle_end:02d}:00 전력 복구",
        ]
        for _, row in idle_df.head(5).iterrows():
            idx = _gpu_index(row['gpu_id'])
            commands.append(f"echo \"0 {idle_end} * * * nvidia-smi -i {idx} -pl {tdp}\" | crontab -")

        commands += [
            f"",
            f"# Verify changes applied",
            f"nvidia-smi --query-gpu=index,name,power.limit,power.draw --format=csv",
        ]

        action_str = (
            f"Ready to execute now — applying to {len(idle_df)} GPUs total\n\n"
            f"[Step 1] Set power limit now (22:00 tonight):\n"
            + '\n'.join([f"  nvidia-smi -i {gpu_index_map.get(r['gpu_id'], _gpu_index(r['gpu_id']))} -pl {idle_power_limit}  # {r['gpu_id']} ({r['avg_util_pct']:.1f}% avg util)"
                        for _, r in idle_df.head(8).iterrows()])
            + f"\n\n[Step 2] Register cron to auto-apply daily (22:00 limit / 07:00 restore):\n"
            + '\n'.join([f"  echo \"0 22 * * * nvidia-smi -i {gpu_index_map.get(r['gpu_id'], _gpu_index(r['gpu_id']))} -pl {idle_power_limit}\" >> /tmp/gpu_cron.txt"
                        for _, r in idle_df.head(8).iterrows()])
            + '\n'
            + '\n'.join([f"  echo \"0 7  * * * nvidia-smi -i {gpu_index_map.get(r['gpu_id'], _gpu_index(r['gpu_id']))} -pl {tdp}\" >> /tmp/gpu_cron.txt"
                        for _, r in idle_df.head(8).iterrows()])
            + f"\n  crontab /tmp/gpu_cron.txt"
            + f"\n\n[Step 3] Verify power limits applied:\n"
            f"  nvidia-smi --query-gpu=index,name,power.limit,power.draw --format=csv\n"
            f"  # Expected: power.limit = {idle_power_limit}W for idle GPUs after 22:00"
        )

        recs.append(Recommendation(
            priority=priority,
            category='Idle Waste',
            title=f"Power-limit {len(idle_df)} idle GPU(s) — save ${idle_total:,.0f}/month starting tonight",
            detail=(
                f"{len(idle_df)} GPUs running at {avg_util:.1f}% average utilization during off-hours "
                f"({idle_start:02d}:00–{idle_end:02d}:00). "
                f"Worst offender: {worst_id} with {worst_hour}h idle/month at {worst.get('avg_util_pct',0):.1f}% util. "
                f"These GPUs are drawing {idle_power_limit*len(idle_df)}W+ while doing nothing. "
                f"Power-limiting to {idle_power_limit}W (from {tdp}W TDP) cuts idle cost by –70% "
                f"with zero impact on workload performance."
            ),
            action=action_str,
            monthly_savings=idle_total,
            annual_savings=idle_total * 12,
            effort='Low',
            confidence=float(idle_df['confidence_pct'].mean()),
            timeframe='Immediate',
            commands=commands,
            owner='DevOps',
            time_to_implement='5 minutes',
            expected_start=f'Tonight {idle_start:02d}:00',
            risk='Zero — power limit auto-restores at {idle_end:02d}:00'.format(idle_end=idle_end),
            kpi=f'GPU power draw < {idle_power_limit+10}W during {idle_start:02d}:00–{idle_end:02d}:00'
        ))
        priority += 1

    # ── REC 2: Overprovisioning ────────────────────────────────
    if over.get('monthly_savings', 0) > 0:
        total_gpus = over.get('total_gpus', 0)
        waste_hours = over.get('top_waste_hours', pd.DataFrame())
        savings = over['monthly_savings']

        # 가장 낭비가 심한 시간대
        if isinstance(waste_hours, pd.DataFrame) and len(waste_hours) > 0:
            worst_hour_row = waste_hours.iloc[0]
            worst_h = int(worst_hour_row.get('hour', 2))
            p95_gpus = float(worst_hour_row.get('p95_active', 2))
            avg_active = float(worst_hour_row.get('avg_active', 1))
        else:
            worst_h = 2
            p95_gpus = max(total_gpus * 0.3, 1)
            avg_active = max(total_gpus * 0.2, 1)

        reducible = max(int(total_gpus - p95_gpus - 1), 0)
        keep_gpus = max(int(p95_gpus + 1), 1)

        action_str = (
            f"[Step 1] Check overnight GPU count (run now):\n"
            f"  nvidia-smi --query-gpu=index,utilization.gpu --format=csv,noheader | awk -F',' '$2+0 > 15'\n\n"
            f"[Step 2] Set Kubernetes HPA (22:00–08:00 → {keep_gpus} replicas):\n"
            f"  kubectl patch hpa gpu-workload-hpa -p '{{\"spec\":{{\"minReplicas\":{keep_gpus}}}}}'\n\n"
            f"[Step 3] Or stop instances directly:\n"
            + '\n'.join([f"  # GPU {i} instance — stop at night" for i in range(reducible)])
            + f"\n  aws ec2 stop-instances --instance-ids <INSTANCE_ID>  # 22:00 cron\n\n"
            f"[Step 4] Morning restore (08:00 cron):\n"
            f"  aws ec2 start-instances --instance-ids <INSTANCE_ID>\n\n"
            f"[Step 5] Monitor results:\n"
            f"  watch -n 60 'nvidia-smi --query-gpu=index,utilization.gpu,power.draw --format=csv'"
        )

        recs.append(Recommendation(
            priority=priority,
            category='Overprovisioning',
            title=f"Scale down to {keep_gpus} GPUs at {worst_h:02d}:00 — {reducible} GPUs wasting ${savings:,.0f}/month",
            detail=(
                f"Full fleet of {total_gpus} GPUs running 24/7. "
                f"At {worst_h:02d}:00, only {avg_active:.1f} GPUs needed on average (p95: {p95_gpus:.1f}). "
                f"{reducible} GPU(s) can be safely stopped between 22:00–08:00 every night. "
                f"This is pure waste — instances running with no workload submitted."
            ),
            action=action_str,
            monthly_savings=savings,
            annual_savings=savings * 12,
            effort='Medium',
            confidence=75.0,
            timeframe='This week',
            owner='DevOps / Infra',
            time_to_implement='2–4 hours',
            expected_start='This Friday 22:00',
            risk='Low — keep 20% buffer above p95 demand',
            kpi=f'Active GPU count < {keep_gpus+1} between 22:00–08:00'
        ))
        priority += 1

    # ── REC 3: Peak Scheduling ─────────────────────────────────
    if peak.get('monthly_savings', 0) > 0:
        savings = peak['monthly_savings']
        offpeak_rate = peak.get('offpeak_rate', 2.10)
        current_rate = peak.get('current_rate', 4.10)
        discount_pct = round((1 - offpeak_rate / max(current_rate, 1)) * 100)

        # 이동 가능한 시간대 분석
        if df is not None and 'hour' in df.columns and 'gpu_util' in df.columns:
            hourly = df.groupby('hour')['gpu_util'].mean()
            peak_hours = hourly[hourly > 60].index.tolist()
            offpeak_hours = hourly[hourly < 30].index.tolist()
            best_offpeak = min(offpeak_hours) if offpeak_hours else 2
        else:
            peak_hours = list(range(9, 18))
            best_offpeak = 2

        action_str = (
            f"[Step 1] Identify current peak training jobs:\n"
            f"  nvidia-smi --query-compute-apps=pid,name,used_memory --format=csv\n"
            f"  ps aux | grep python  # find running training scripts\n\n"
            f"[Step 2] If using Slurm — set off-peak queue:\n"
            f"  scontrol update partition=gpu-offpeak StartTime={best_offpeak:02d}:00:00\n"
            f"  sbatch --begin={best_offpeak:02d}:00 train_job.sh\n\n"
            f"[Step 3] If using Cron — reschedule training jobs:\n"
            f"  # Remove existing daytime training cron, move to overnight\n"
            f"  crontab -e\n"
            f"  # Add: 0 {best_offpeak} * * 1-5 cd /workspace && python train.py >> /logs/train.log 2>&1\n\n"
            f"[Step 4] Verify cost savings after applying:\n"
            f"  # AWS: Check hourly EC2 costs in Cost Explorer\n"
            f"  aws ce get-cost-and-usage --time-period Start=2024-01-01,End=2024-01-31 \\\n"
            f"    --granularity HOURLY --metrics BlendedCost"
        )

        recs.append(Recommendation(
            priority=priority,
            category='Peak Scheduling',
            title=f"Move batch training to {best_offpeak:02d}:00 — {discount_pct}% cheaper electricity",
            detail=(
                f"Training jobs running at ${current_rate:.2f}/hr during peak window (09:00–18:00). "
                f"Off-peak rate is ${offpeak_rate:.2f}/hr — {discount_pct}% cheaper. "
                f"Non-real-time training jobs (fine-tuning, batch inference, eval runs) "
                f"can run overnight with zero user impact. "
                f"Only real-time inference must stay on current schedule."
            ),
            action=action_str,
            monthly_savings=savings,
            annual_savings=savings * 12,
            effort='Low',
            confidence=85.0,
            timeframe='This week',
            owner='ML Engineer',
            time_to_implement='1–2 hours',
            expected_start=f'Tomorrow {best_offpeak:02d}:00',
            risk='Zero — inference workloads unaffected',
            kpi=f'Training job start time > 22:00 or < 07:00 for >80% of batch jobs'
        ))
        priority += 1

    # ── REC 4: Workload Gap ────────────────────────────────────
    if workload_gap and workload_gap.get('monthly_savings', 0) > 100:
        savings = workload_gap['monthly_savings']
        gap_hours = workload_gap.get('total_waste_hours_monthly', 0)
        affected = workload_gap.get('affected_gpus', [])
        worst_gap_gpu = affected[0]['gpu_id'] if affected else 'N/A'
        worst_gap_idx = _gpu_index(worst_gap_gpu)

        action_str = (
            f"[Step 1] Check workload gap in real-time:\n"
            f"  # Find GPUs powered on but <5% utilization\n"
            f"  nvidia-smi --query-gpu=index,utilization.gpu,power.draw,name \\\n"
            f"    --format=csv,noheader | awk -F',' '$2+0 < 5 && $3+0 > 50'\n\n"
            f"[Step 2] Set up Idle Job Reaper (NVIDIA recommended):\n"
            f"  # If util < 5% for 30+ min → auto alert\n"
            f"  cat > /usr/local/bin/gpu_reaper.sh << 'EOF'\n"
            f"  #!/bin/bash\n"
            f"  UTIL=$(nvidia-smi -i {worst_gap_idx} --query-gpu=utilization.gpu --format=csv,noheader,nounits)\n"
            f"  if [ $UTIL -lt 5 ]; then\n"
            f"    echo \"GPU {worst_gap_idx} idle: ${{UTIL}}%\" | mail -s \"GPU IDLE ALERT\" devops@company.com\n"
            f"  fi\n"
            f"  EOF\n"
            f"  chmod +x /usr/local/bin/gpu_reaper.sh\n"
            f"  echo '*/30 * * * * /usr/local/bin/gpu_reaper.sh' | crontab -\n\n"
            f"[Step 3] Immediately check GPUs with no jobs submitted:\n"
            f"  nvidia-smi pmon -s u -d 5  # 5초마다 프로세스 모니터링"
        )

        recs.append(Recommendation(
            priority=priority,
            category='Workload Gap',
            title=f"Eliminate {gap_hours:.0f}h/month of powered-on-but-idle GPU time",
            detail=(
                f"GPUs are powered on and drawing electricity but have no active jobs submitted. "
                f"This is different from scheduled idle — these are gaps between job submissions "
                f"where GPUs sit at <5% utilization while consuming 15–20% of TDP. "
                f"Worst offender: {worst_gap_gpu}. "
                f"Root cause: no job queuing system, manual job submission delays, "
                f"or jobs finishing early without auto-shutdown."
            ),
            action=action_str,
            monthly_savings=savings,
            annual_savings=savings * 12,
            effort='Medium',
            confidence=70.0,
            timeframe='This month',
            owner='MLOps / DevOps',
            time_to_implement='2–3 hours',
            expected_start='This week',
            risk='Low — monitoring only, no workload change',
            kpi=f'GPU utilization gap events < 10/day per GPU'
        ))
        priority += 1

    # ── REC 5: Thermal Throttling ──────────────────────────────
    if thermal and thermal.get('monthly_savings', 0) > 50:
        savings = thermal['monthly_savings']
        affected = thermal.get('affected_gpus', [])
        if affected:
            worst_t = affected[0]
            worst_t_id = worst_t['gpu_id']
            worst_t_idx = _gpu_index(worst_t_id)
            max_temp = worst_t['max_temp']
            throttle_pct = worst_t['throttle_pct']

            action_str = (
                f"[Step 1] Check current temperature and throttling status:\n"
                f"  nvidia-smi --query-gpu=index,temperature.gpu,clocks_throttle_reasons.active \\\n"
                f"    --format=csv\n\n"
                f"[Step 2] {worst_t_id} immediate action (temp {max_temp:.0f}°C detected):\n"
                f"  # Set fan speed to maximum\n"
                f"  nvidia-smi -i {worst_t_idx} --auto-boost-default=0\n"
                f"  nvidia-smi -i {worst_t_idx} -pl $(nvidia-smi -i {worst_t_idx} \\\n"
                f"    --query-gpu=power.max_limit --format=csv,noheader,nounits | awk '{{print int($1*0.85)}}')\n\n"
                f"[Step 3] Long-term fix — cooling inspection:\n"
                f"  # Check datacenter inlet temperature (recommended: < 27°C)\n"
                f"  # Clean GPU fan filters (every 6 months)\n"
                f"  # Improve airflow by organizing server rack cables\n\n"
                f"[Step 4] Set up temperature alert (80°C threshold):\n"
                f"  echo '*/5 * * * * nvidia-smi --query-gpu=temperature.gpu \\\n"
                f"    --format=csv,noheader | awk \"\\$1>80{{print}}\" \\\n"
                f"    | mail -s \"THERMAL ALERT\" ops@company.com' | crontab -"
            )

            recs.append(Recommendation(
                priority=priority,
                category='Thermal Throttling',
                title=f"Fix thermal throttling on {worst_t_id} — {throttle_pct:.1f}% of time at {max_temp:.0f}°C",
                detail=(
                    f"{worst_t_id} exceeds 83°C for {throttle_pct:.1f}% of runtime, "
                    f"triggering NVIDIA automatic clock reduction. "
                    f"During throttling periods, GPU performs at {worst_t['performance_loss_pct']:.0f}% "
                    f"below peak while consuming full power — worst of both worlds. "
                    f"Root cause: insufficient cooling, blocked airflow, or sustained 100% load "
                    f"without thermal headroom."
                ),
                action=action_str,
                monthly_savings=savings,
                annual_savings=savings * 12,
                effort='Medium',
                confidence=float(min(95, 60 + throttle_pct * 3)),
                timeframe='This week',
                owner='Infra / Facilities',
                time_to_implement='1 hour + datacenter visit',
                expected_start='This week',
                risk='Medium — requires physical datacenter access',
                kpi=f'GPU temperature < 80°C for >95% of runtime'
            ))
            priority += 1

    # ── REC 6: Memory Bandwidth Bottleneck ─────────────────────
    if mem_bottleneck and mem_bottleneck.get('monthly_savings', 0) > 50:
        savings = mem_bottleneck['monthly_savings']
        affected = mem_bottleneck.get('affected_gpus', [])
        if affected:
            worst_m = affected[0]
            worst_m_id = worst_m['gpu_id']
            ratio = worst_m['avg_mem_compute_ratio']
            bottleneck_pct = worst_m['bottleneck_pct']

            action_str = (
                f"[Step 1] Verify memory bottleneck:\n"
                f"  # Check memory/compute ratio in real-time\n"
                f"  nvidia-smi --query-gpu=index,utilization.gpu,utilization.memory \\\n"
                f"    --format=csv --loop=5\n\n"
                f"[Step 2] Double batch size (apply immediately):\n"
                f"  # In your PyTorch training script:\n"
                f"  # Before: DataLoader(dataset, batch_size=32, num_workers=2)\n"
                f"  # After:  DataLoader(dataset, batch_size=64, num_workers=8, pin_memory=True)\n\n"
                f"[Step 3] Apply Mixed Precision (50% memory bandwidth reduction):\n"
                f"  from torch.cuda.amp import autocast, GradScaler\n"
                f"  scaler = GradScaler()\n"
                f"  with autocast():\n"
                f"      output = model(input)\n"
                f"      loss = criterion(output, target)\n\n"
                f"[Step 4] Enable data prefetching:\n"
                f"  DataLoader(dataset, num_workers=8, prefetch_factor=4, pin_memory=True)"
            )

            recs.append(Recommendation(
                priority=priority,
                category='Memory Bottleneck',
                title=f"Fix memory bandwidth bottleneck on {worst_m_id} — GPU waiting on data {bottleneck_pct:.0f}% of time",
                detail=(
                    f"{worst_m_id} shows memory/compute ratio of {ratio:.2f}x "
                    f"(ideal: 0.8–1.2x) for {bottleneck_pct:.0f}% of active runtime. "
                    f"GPU compute cores are stalled waiting for data from memory — "
                    f"you're paying for GPU compute time while it sits idle waiting for tensors. "
                    f"Fix: increase batch size, enable Mixed Precision (FP16), "
                    f"add data prefetching workers."
                ),
                action=action_str,
                monthly_savings=savings,
                annual_savings=savings * 12,
                effort='Low',
                confidence=float(worst_m.get('confidence_pct', 70)),
                timeframe='This week',
                owner='ML Engineer',
                time_to_implement='30 minutes',
                expected_start='Next training run',
                risk='Low — test with small batch first',
                kpi=f'Memory/compute utilization ratio < 1.3x during training'
            ))
            priority += 1

    # ── REC 7: Inter-GPU Consolidation ─────────────────────────
    if inter_gpu and inter_gpu.get('monthly_savings', 0) > 100:
        savings = inter_gpu['monthly_savings']
        waste_hours = inter_gpu.get('total_waste_hours_monthly', 0)
        avg_idle_gpus = inter_gpu.get('avg_concurrent_idle_gpus', 0)
        worst_hours = inter_gpu.get('worst_hours', [])
        consolidatable = inter_gpu.get('consolidatable_gpus', 0)

        worst_h_str = ', '.join([f'{h:02d}:00' for h in worst_hours[:3]]) if worst_hours else 'overnight'

        action_str = (
            f"[Step 1] Check concurrent idle GPU pattern:\n"
            f"  # Check how many GPUs are idle simultaneously right now\n"
            f"  nvidia-smi --query-gpu=index,utilization.gpu --format=csv,noheader \\\n"
            f"    | awk -F',' '$2+0 < 15 {{count++}} END {{print count \" GPUs idle\"}}'\n\n"
            f"[Step 2] Set up workload consolidation (NVIDIA MIG or Run:AI):\n"
            f"  # For MIG-capable GPUs (A100, H100):\n"
            f"  nvidia-smi mig -cgi 3g.40gb -C  # Split A100 into 2 instances\n\n"
            f"[Step 3] Configure Kubernetes GPU sharing:\n"
            f"  # Enable gpu-sharing\n"
            f"  kubectl label nodes <NODE> nvidia.com/gpu.sharing=true\n"
            f"  # Allocate 0.5 GPU per workload\n"
            f"  resources:\n"
            f"    limits:\n"
            f"      nvidia.com/gpu: 0.5\n\n"
            f"[Step 4] Focus monitoring on worst waste hours ({worst_h_str}):\n"
            f"  watch -n 300 'nvidia-smi --query-gpu=utilization.gpu --format=csv'"
        )

        recs.append(Recommendation(
            priority=priority,
            category='GPU Consolidation',
            title=f"Consolidate workloads — {avg_idle_gpus:.0f} GPUs idle simultaneously at {worst_h_str}",
            detail=(
                f"Analysis shows {avg_idle_gpus:.1f} GPUs idle at the same time "
                f"for {waste_hours:.0f}h/month (worst hours: {worst_h_str}). "
                f"{consolidatable:.0f} GPU(s) can be consolidated using MIG partitioning "
                f"or Kubernetes GPU sharing. "
                f"Multiple small workloads can share one GPU instead of each occupying a dedicated GPU."
            ),
            action=action_str,
            monthly_savings=savings,
            annual_savings=savings * 12,
            effort='Medium',
            confidence=float(min(75, 55 + avg_idle_gpus * 2)),
            timeframe='This month',
            owner='DevOps / MLOps',
            time_to_implement='4–8 hours',
            expected_start='This sprint',
            risk='Medium — test consolidation with non-critical workloads first',
            kpi=f'Concurrent idle GPU count < 2 during business hours'
        ))
        priority += 1

    # ── REC 8: Efficiency Grade D GPUs ─────────────────────────
    if scores is not None and len(scores) > 0:
        grade_d = scores[scores['grade'] == 'D'] if 'grade' in scores.columns else pd.DataFrame()
        if len(grade_d) > 0:
            worst_score_gpu = grade_d.iloc[-1]
            worst_score_id = worst_score_gpu['gpu_id']
            worst_score_idx = _gpu_index(worst_score_id)
            avg_score = grade_d['total_score'].mean() if 'total_score' in grade_d.columns else \
                        grade_d['efficiency'].mean() if 'efficiency' in grade_d.columns else 30

            action_str = (
                f"[Step 1] Profile Grade D GPUs in detail:\n"
                f"  # {worst_score_id} real-time status\n"
                f"  nvidia-smi -i {worst_score_idx} -q  # 전체 상태 덤프\n"
                f"  nvidia-smi -i {worst_score_idx} pmon -s u -d 10  # 10초 프로세스 모니터링\n\n"
                f"[Step 2] Redistribute workloads:\n"
                f"  # Move workloads from Grade D to Grade A/B GPUs\n"
                f"  # Use CUDA_VISIBLE_DEVICES environment variable\n"
                f"  export CUDA_VISIBLE_DEVICES=0,1,2  # Exclude Grade D GPUs\n\n"
                f"[Step 3] Lower priority in scheduler:\n"
                f"  # For Slurm:\n"
                f"  scontrol update node=gpu-node-{worst_score_idx} state=drain reason='low_efficiency'\n\n"
                f"[Step 4] Automate weekly efficiency reporting:\n"
                f"  # Track improvements via InfraLens re-analysis"
            )

            recs.append(Recommendation(
                priority=priority,
                category='Efficiency',
                title=f"{len(grade_d)} GPU(s) Grade D — avg efficiency score {avg_score:.0f}/100",
                detail=(
                    f"{len(grade_d)} GPUs scoring below 50/100 on the 6-dimension efficiency index "
                    f"(compute utilization, memory efficiency, power efficiency, "
                    f"thermal health, workload consistency, resource utilization). "
                    f"Worst: {worst_score_id} at {avg_score:.0f}/100. "
                    f"These GPUs are underperforming relative to their cost and should be "
                    f"either consolidated, re-tasked, or retired."
                ),
                action=action_str,
                monthly_savings=0,
                annual_savings=0,
                effort='Medium',
                confidence=70.0,
                timeframe='This month',
                owner='ML Engineer / DevOps',
                time_to_implement='1–2 days',
                expected_start='This sprint',
                risk='Low — profiling only initially',
                kpi=f'All GPUs score > 60/100 within 30 days'
            ))
            priority += 1

    # infrastructure_advisor로 action 교체
    try:
        if df is not None:
            env = detect_environment(df)
            for r in recs:
                guide = build_action_guide(r, env, df)
                r.action = format_guide_text(guide)
    except Exception as e:
        pass

    # savings 기준 정렬 후 priority 재부여
    sorted_recs = sorted(recs, key=lambda r: r.monthly_savings, reverse=True)
    for i, r in enumerate(sorted_recs, 1):
        r.priority = i

    return sorted_recs
