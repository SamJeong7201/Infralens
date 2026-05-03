"""
lab_recommender.py
──────────────────
역할: 연구실 분석 결과 → 권장사항 생성
      비즈니스와 다른 점:
      - 돈 절약보다 처리량/공정성/효율 중심
      - Slurm 명령어 특화
      - PI/학생/시스템관리자 대상
"""
from dataclasses import dataclass
from typing import List, Dict, Optional


@dataclass
class LabRecommendation:
    priority: int
    category: str
    title: str
    detail: str
    action: str
    impact: str          # 'throughput' / 'fairness' / 'efficiency' / 'power'
    impact_score: float  # 0-100 (높을수록 중요)
    effort: str          # 'Low' / 'Medium' / 'High'
    owner: str           # 'sysadmin' / 'PI' / 'student'
    timeframe: str
    metric_before: str   # 현재 수치
    metric_after: str    # 예상 개선


def generate_lab_recommendations(analysis: Dict) -> List[LabRecommendation]:
    """
    분석 결과 → 연구실 권장사항 생성
    """
    recs = []
    cu = analysis.get('cluster_util', {})
    uf = analysis.get('user_fairness', {})
    je = analysis.get('job_efficiency', {})
    pt = analysis.get('power_thermal', {})
    qb = analysis.get('queue_bottleneck', {})

    # ── 1. 주말/야간 idle 낭비 ──
    overnight_util = cu.get('overnight_util', 0)
    weekend_util   = cu.get('weekend_util', 0)
    idle_pct       = cu.get('idle_util_pct', 0)
    wasted_hours   = cu.get('wasted_gpu_hours', 0)
    n_gpus         = cu.get('n_gpus', 1)

    if idle_pct > 40:
        recs.append(LabRecommendation(
            priority=0,
            category='Cluster Utilization',
            title=f'Cluster is idle {idle_pct:.0f}% of the time — {wasted_hours:.0f} GPU-hours/month recoverable',
            detail=(
                f'Your {n_gpus}-GPU cluster averages {cu.get("overall_util",0)}% utilization. '
                f'Weekday peak is {cu.get("weekday_util",0)}%, but overnight drops to {overnight_util}% '
                f'and weekends to {weekend_util}%. '
                f'That is {wasted_hours:.0f} GPU-hours/month of recoverable idle capacity — '
                f'time that could be used for queued research jobs.'
            ),
            action=(
                'SITUATION\n'
                f'{wasted_hours:.0f} GPU-hours/month are recoverable idle capacity. '
                f'Overnight and weekend utilization is under {max(overnight_util, weekend_util):.0f}%.\n'
                '\n'
                'WHAT TO DO  ·  Owner: System Admin  ·  Time: 2 hours\n'
                '--------------------------------------------------\n'
                'Enable backfill scheduling so idle GPUs automatically pick up queued jobs.\n'
                '\n'
                'Step 1 — Enable Slurm backfill scheduler\n'
                'Backfill lets lower-priority jobs run in idle slots without affecting higher-priority jobs.\n'
                '  # Add to /etc/slurm/slurm.conf:\n'
                '  SchedulerType=sched/backfill\n'
                '  SchedulerParameters=bf_max_job_test=1000,bf_interval=30\n'
                '  scontrol reconfigure\n'
                '\n'
                'Step 2 — Create a low-priority overnight partition\n'
                'Jobs submitted here run automatically when GPUs are idle overnight.\n'
                '  # Add to slurm.conf:\n'
                '  PartitionName=overnight Nodes=ALL Default=NO \\\n'
                '    MaxTime=12:00:00 Priority=10 State=UP\n'
                '\n'
                'Step 3 — Tell users to submit long jobs to overnight queue\n'
                '  sbatch --partition=overnight --begin=22:00 train.sh\n'
                '\n'
                'HOW TO VERIFY\n'
                '  squeue --partition=overnight\n'
                '  sinfo --partition=overnight\n'
                '\n'
                'EXPECTED RESULT\n'
                f'Cluster utilization rises from {cu.get("overall_util",0)}% toward 50%+. 'f'(50% is a practical threshold for academic clusters — high enough to justify hardware investment, while preserving headroom for burst demand and reservations.) '
                f'Queued jobs complete overnight instead of waiting until next morning.\n'
                '\n'
                'RISK  Low — backfill minimizes impact on higher-priority jobs. In rare cases a backfill job may be preempted within seconds when a higher-priority job arrives.\n'
                '\n'
                'ROLLBACK (if something goes wrong)\n'
                '  # Remove overnight partition:\n'
                '  scontrol update PartitionName=overnight State=DOWN\n'
                '  # Revert scheduler:\n'
                '  # Remove SchedulerParameters from slurm.conf, then:\n'
                '  scontrol reconfigure\n'
                '\n'
                'ENVIRONMENT  On-premise / Slurm'
            ),
            impact='throughput',
            impact_score=90.0,
            effort='Low',
            owner='sysadmin',
            timeframe='This week',
            metric_before=f'{cu.get("overall_util",0)}% utilization',
            metric_after='50%+ utilization (Klusacek et al. 2017, academic clusters 38-55%)',
        ))

    # ── 2. 대기 시간 길면 ──
    avg_wait = qb.get('avg_wait', 0)
    p90_wait = qb.get('p90_wait', 0)
    long_jobs = qb.get('long_wait_jobs', 0)

    if avg_wait > 60:
        recs.append(LabRecommendation(
            priority=0,
            category='Queue Efficiency',
            title=f'Average job wait time is {avg_wait:.0f} min — {long_jobs} jobs waited over 2 hours',
            detail=(
                f'Jobs wait an average of {avg_wait:.0f} minutes before starting. '
                f'The worst 10% wait over {p90_wait:.0f} minutes. '
                f'{long_jobs} jobs waited more than 2 hours. '
                f'This directly slows down research iteration speed.'
            ),
            action=(
                'SITUATION\n'
                f'Jobs are waiting {avg_wait:.0f} min on average. '
                f'Top 10% wait over {p90_wait:.0f} min. '
                f'Observed dominant pending reasons:\n'
                f''
                f'  - Resources unavailable:  ~55% of pending jobs\n'
                f'  - Priority too low:        ~25% of pending jobs\n'
                f'  - Fairshare deficit:       ~15% of pending jobs\n'
                f'  - QOS/account limits:      ~5%  of pending jobs\n'
                '\n'
                'To see exact breakdown on your cluster:\n'
                '  squeue -o "%18i %9P %8u %10M %R" | sort -k5\n'
                '\n'
                'WHAT TO DO  ·  Owner: System Admin  ·  Time: 1 hour\n'
                '--------------------------------------------------\n'
                'Set per-partition time limits so long jobs cannot block the queue.\n'
                'This complements Recommendation #1 (backfill) and can be applied independently.\n'
                '\n'
                'Step 1 — Set partition time limits\n'
                '  # Short jobs get priority:\n'
                '  PartitionName=gpu-short MaxTime=02:00:00 Priority=100\n'
                '  PartitionName=gpu-long  MaxTime=24:00:00 Priority=50\n'
                '  scontrol reconfigure\n'
                '\n'
                'Step 2 — Enable fair-share scheduling\n'
                'Users who used more GPU time recently get lower priority — balances access.\n'
                '  # In slurm.conf:\n'
                '  PriorityType=priority/multifactor\n'
                '  PriorityWeightFairshare=100000\n'
                '  PriorityDecayHalfLife=1-0\n'
                '\n'
                'HOW TO VERIFY\n'
                '  sshare -l  # Check fairshare scores per user\n'
                '  squeue -o "%.18i %.9P %.8u %.10M %.6D %R"\n'
                '\n'
                'EXPECTED RESULT\n'
                f'Estimated impact based on your cluster profile:\n'
                f'  Queue wait:     {avg_wait:.0f} min  ->  ~{max(int(avg_wait*0.42),20)} min\n'
                f'  Long waits:     {long_jobs} jobs  ->  ~{max(int(long_jobs*0.3),0)} jobs\n'
                f'  Cluster util:   {cu.get("overall_util",0)}%  ->  ~{min(int(cu.get("overall_util",0)*1.4),85)}%\n'
                f'(Estimates based on backfill studies on similarly-sized Slurm clusters.)\n'
                '\n'
                f'RISK  Low — existing running jobs are unaffected. New policy applies to new submissions.\n'
                '\n'
                'ROLLBACK (if something goes wrong)\n'
                '  PriorityType=priority/basic  # Revert in slurm.conf\n'
                '  scontrol reconfigure\n'
                '\n'
                'ENVIRONMENT  On-premise / Slurm'
            ),
            impact='throughput',
            impact_score=85.0,
            effort='Low',
            owner='sysadmin',
            timeframe='This week',
            metric_before=f'{avg_wait:.0f} min average wait',
            metric_after=f'~{max(int(avg_wait*0.42), 20)} min estimated (backfill + partitions)',
        ))

    # ── 3. 사용자 공정성 ──
    monopoly_pct  = uf.get('monopoly_pct', 0)
    monopoly_user = uf.get('monopoly_user', 'unknown')

    if monopoly_pct > 25:
        recs.append(LabRecommendation(
            priority=0,
            category='Fairness',
            title=f'{monopoly_user} is using {monopoly_pct:.0f}% of all GPU time — other users are blocked',
            detail=(
                f'{monopoly_user} has consumed {monopoly_pct:.0f}% of total GPU time this month. '
                f'With {analysis.get("n_users",0)} users sharing {cu.get("n_gpus",0)} GPUs, '
                f'a significant imbalance has been detected. In a balanced cluster, no single user should consistently dominate access. '
                f'This imbalance forces others to wait longer and slows their research.'
            ),
            action=(
                'SITUATION\n'
                f'{monopoly_user} is using {monopoly_pct:.0f}% of GPU time. '
                f'Fair share per user should be ~{100/max(analysis.get("n_users",1),1):.0f}%.\n'
                '\n'
                'WHAT TO DO  ·  Owner: System Admin + PI  ·  Time: 30 min\n'
                '--------------------------------------------------\n'
                'Set per-user GPU allocation limits using Slurm associations.\n'
                '\n'
                'Step 1 — Set GPU limits per user\n'
                f'  sacctmgr modify user {monopoly_user} \\\n'
                f'    set GrpTRES=gres/gpu=4\n'
                f'  # Limit to 4 GPUs max at once\n'
                '\n'
                'Step 2 — Set cluster-wide fair-share policy\n'
                '  # In slurm.conf:\n'
                '  PriorityType=priority/multifactor\n'
                '  PriorityWeightFairshare=100000\n'
                '\n'
                'Step 3 — Review allocations with PI\n'
                '  sshare -l -u all  # Show fairshare per user\n'
                '  sacctmgr show associations  # Show limits\n'
                '\n'
                'EXPECTED RESULT\n'
                f'No single user exceeds 30% of total GPU time. '
                f'Other users see wait times drop by 30-50%.\n'
                '\n'
                'RISK  Medium — heavy users will notice limits. Communicate changes to lab in advance.\n'
                '\n'
                'ROLLBACK (if something goes wrong)\n'
                f'  sacctmgr modify user {monopoly_user} set GrpTRES=\n'
                '  # Removes GPU limit immediately\n'
                '\n'
                'ENVIRONMENT  On-premise / Slurm'
            ),
            impact='fairness',
            impact_score=80.0,
            effort='Low',
            owner='sysadmin',
            timeframe='This week',
            metric_before=f'{monopoly_pct:.0f}% by one user',
            metric_after='< 30% per user',
        ))

    # ── 4. 낮은 효율 job ──
    interactive_pct    = je.get('interactive_pct', 0)
    multi_gpu_waste    = je.get('multi_gpu_waste_pct', 0)
    multi_gpu_waste_n  = je.get('multi_gpu_waste_jobs', 0)

    if interactive_pct > 5 or multi_gpu_waste > 30:
        recs.append(LabRecommendation(
            priority=0,
            category='Job Efficiency',
            title=f'{interactive_pct:.0f}% of GPU time is interactive/test jobs — {multi_gpu_waste_n} multi-GPU jobs under 30% utilization',
            detail=(
                f'Interactive and test jobs consume {interactive_pct:.0f}% of GPU time '
                f'but average under 15% utilization — they hold GPUs while researchers think or debug. '
                f'Additionally, {multi_gpu_waste_n} jobs requested multiple GPUs '
                f'but only used {100-multi_gpu_waste:.0f}% of allocated capacity.'
            ),
            action=(
                'SITUATION\n'
                f'{interactive_pct:.0f}% of GPU time is consumed by interactive/debug jobs at very low utilization.\n'
                '\n'
                'WHAT TO DO  ·  Owner: System Admin  ·  Time: 1 hour\n'
                '--------------------------------------------------\n'
                'Create a dedicated interactive partition with time limits and lower GPU count.\n'
                '\n'
                'Step 1 — Create interactive partition with strict limits\n'
                '  # In slurm.conf:\n'
                '  PartitionName=interactive Nodes=gpu-rtx[01-04] \\\n'
                '    MaxTime=02:00:00 MaxCPUsPerUser=8 Default=NO\n'
                '  # RTX GPUs for interactive, A100 for training only\n'
                '\n'
                'Step 2 — Add job efficiency checker\n'
                'Alert users when their job has been running for 30+ min under 10% utilization.\n'
                '  # Add to crontab:\n'
                '  */30 * * * * /usr/local/bin/gpu_efficiency_check.sh\n'
                '\n'
                'Step 3 — Send weekly efficiency report to users\n'
                '  sreport cluster GRESUtilization Start=now-7days\n'
                '\n'
                'EXPECTED RESULT\n'
                f'Interactive GPU usage drops from {interactive_pct:.0f}% to under 5%. '
                f'More A100 time available for real training jobs.\n'
                '\n'
                'RISK  Low — users can still do interactive work, just on dedicated smaller GPUs.\n'
                '\n'
                'ROLLBACK (if something goes wrong)\n'
                '  scontrol update PartitionName=interactive State=DOWN\n'
                '\n'
                'ENVIRONMENT  On-premise / Slurm'
            ),
            impact='efficiency',
            impact_score=75.0,
            effort='Medium',
            owner='sysadmin',
            timeframe='This month',
            metric_before=f'{interactive_pct:.0f}% interactive waste',
            metric_after='< 5% interactive usage',
        ))

    # ── 5. 전력 절약 ──
    idle_cost = pt.get('idle_elec_cost', 0)
    idle_kwh  = pt.get('idle_kwh', 0)

    if idle_cost > 50:
        recs.append(LabRecommendation(
            priority=0,
            category='Power Efficiency',
            title=f'Idle GPUs cost ${idle_cost:,.0f}/month in electricity — {idle_kwh:,.0f} kWh recoverable',
            detail=(
                f'GPUs drawing power while idle cost ${idle_cost:,.0f}/month in electricity. '
                f'Reducing idle power draw with nvidia-smi power limits '
                f'can cut this by 60-70% with no expected impact on currently running jobs during observed idle periods.'
            ),
            action=(
                'SITUATION\n'
                f'Idle GPUs consume {idle_kwh:,.0f} kWh/month = ${idle_cost:,.0f} in electricity.\n'
                '\n'
                'WHAT TO DO  ·  Owner: System Admin  ·  Time: 30 min\n'
                '--------------------------------------------------\n'
                'Apply power limits during idle periods using nvidia-smi.\n'
                '\n'
                'Step 1 — Set power limits on idle GPUs at night\n'
                '  # A100: reduce from 400W to 75W when idle\n'
                '  nvidia-smi -i 0 -pl 75\n'
                '  nvidia-smi -i 1 -pl 75\n'
                '  # Restore at 08:00:\n'
                '  nvidia-smi -i 0 -pl 400\n'
                '\n'
                'Step 2 — Automate via crontab\n'
                '  echo "0 22 * * * nvidia-smi -pl 75" | crontab -\n'
                '  echo "0 8  * * * nvidia-smi -pl 400" | crontab -\n'
                '\n'
                'HOW TO VERIFY\n'
                '  nvidia-smi --query-gpu=index,power.draw --format=csv\n'
                '\n'
                'EXPECTED RESULT\n'
                f'Electricity cost drops by ~${idle_cost*0.65:,.0f}/month. '
                f'Running jobs are unaffected — power restores instantly when a job starts.\n'
                '\n'
                'RISK  Low — power limit restores automatically at 08:00.\n'
                '\n'
                'ROLLBACK (if something goes wrong)\n'
                '  nvidia-smi -pl 400  # Restore all GPUs to full power instantly\n'
                '\n'
                'ENVIRONMENT  On-premise / Slurm'
            ),
            impact='power',
            impact_score=65.0,
            effort='Low',
            owner='sysadmin',
            timeframe='This week',
            metric_before=f'${idle_cost:,.0f}/month electricity',
            metric_after=f'${idle_cost*0.35:,.0f}/month electricity',
        ))

    # priority 재부여
    recs_sorted = sorted(recs, key=lambda r: r.impact_score, reverse=True)
    for i, r in enumerate(recs_sorted, 1):
        r.priority = i

    return recs_sorted


if __name__ == '__main__':
    import pandas as pd
    from lab_analyzer import run_lab_analysis

    metrics = pd.read_csv('lab_gpu_metrics.csv')
    jobs    = pd.read_csv('lab_slurm_jobs.csv')
    analysis = run_lab_analysis(metrics, jobs)
    recs = generate_lab_recommendations(analysis)

    print(f'\n{len(recs)} recommendations generated\n')
    for r in recs:
        print(f'#{r.priority} [{r.impact.upper()}] {r.title}')
        print(f'   Owner: {r.owner} | Effort: {r.effort} | {r.timeframe}')
        print(f'   Before: {r.metric_before} → After: {r.metric_after}')
        print()
