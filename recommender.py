import pandas as pd
import numpy as np
from dataclasses import dataclass
from typing import List

@dataclass
class Recommendation:
    priority: int
    category: str
    title: str
    detail: str
    action: str
    monthly_savings: float
    annual_savings: float
    effort: str       # Low / Medium / High
    confidence: float
    timeframe: str    # Immediate / This week / This month

def generate_recommendations(
    idle_df: pd.DataFrame,
    peak: dict,
    over: dict,
    sim: dict,
    scores: pd.DataFrame,
) -> List[Recommendation]:
    recs = []
    priority = 1

    # ── Rec 1: Idle GPU ──
    if len(idle_df) > 0:
        idle_total = idle_df['monthly_savings'].sum()
        worst_gpu  = idle_df.iloc[0]
        worst_hour = int(worst_gpu['worst_hour'])
        recs.append(Recommendation(
            priority=priority,
            category='Idle Waste',
            title=f"Enable power-saving on {len(idle_df)} idle GPU(s)",
            detail=(f"{len(idle_df)} GPUs averaging {idle_df['avg_util_pct'].mean():.1f}% "
                    f"utilization during off-hours. Worst offender: {worst_gpu['gpu_id']} "
                    f"at {worst_hour:02d}:00 with {worst_gpu['idle_hours']}h idle/month."),
            action=(f"Schedule automatic power-saving mode from "
                    f"{worst_hour:02d}:00–{(worst_hour+4)%24:02d}:00 daily. "
                    f"Use 'nvidia-smi --auto-boost-default=0' and set persistence mode."),
            monthly_savings=idle_total,
            annual_savings=idle_total * 12,
            effort='Low',
            confidence=float(idle_df['confidence_pct'].mean()),
            timeframe='Immediate'
        ))
        priority += 1

    # ── Rec 2: Peak Scheduling ──
    if peak.get('peak_hours_count', 0) > 0 and peak['monthly_savings'] > 0:
        recs.append(Recommendation(
            priority=priority,
            category='Peak Scheduling',
            title=f"Reschedule {peak['peak_hours_count']} peak-hour training jobs",
            detail=(f"Training jobs running at ${peak['peak_rate']}/hr during peak window. "
                    f"Off-peak rate is ${peak['offpeak_rate']}/hr — "
                    f"{((peak['peak_rate'] - peak['offpeak_rate']) / peak['peak_rate'] * 100):.0f}% cheaper."),
            action=(f"Move batch training jobs to 02:00–06:00 window. "
                    f"Use job scheduler (cron/Slurm) to queue training after 22:00. "
                    f"Real-time inference stays on current schedule."),
            monthly_savings=peak['monthly_savings'],
            annual_savings=peak['monthly_savings'] * 12,
            effort='Low',
            confidence=85.0,
            timeframe='This week'
        ))
        priority += 1

    # ── Rec 3: Overprovisioning ──
    if over.get('monthly_savings', 0) > 0 and len(over.get('top_waste_hours', pd.DataFrame())) > 0:
        top = over['top_waste_hours'].iloc[0]
        recs.append(Recommendation(
            priority=priority,
            category='Overprovisioning',
            title=f"Scale down GPU fleet during off-peak hours",
            detail=(f"Fleet running at full capacity 24/7. "
                    f"At {int(top['hour']):02d}:00, only {top['avg_active']} GPUs "
                    f"needed on average (p95: {top['p95_demand']}). "
                    f"{int(top['reducible'])} GPU(s) can be safely removed."),
            action=(f"Implement auto-scaling: reduce to {int(top['needed_w_buffer'])} GPUs "
                    f"between 22:00–08:00. Use Kubernetes HPA or custom scaling policy. "
                    f"Keep 20% buffer above p95 demand."),
            monthly_savings=over['monthly_savings'],
            annual_savings=over['monthly_savings'] * 12,
            effort='Medium',
            confidence=75.0,
            timeframe='This month'
        ))
        priority += 1

    # ── Rec 4: Low efficiency GPUs ──
    if len(scores) > 0:
        low_eff = scores[scores['grade'].isin(['C', 'D'])]
        if len(low_eff) > 0:
            recs.append(Recommendation(
                priority=priority,
                category='Efficiency',
                title=f"{len(low_eff)} GPU(s) below efficiency threshold (Grade C or lower)",
                detail=(f"Average efficiency score: {low_eff['efficiency'].mean():.0f}/100. "
                        f"High waste percentage ({low_eff['waste_pct'].mean():.1f}%) "
                        f"indicates underutilized capacity during active hours."),
                action=(f"Consolidate workloads onto fewer GPUs during low-demand periods. "
                        f"Review job scheduling to improve GPU packing. "
                        f"Target: efficiency score > 70 (Grade B)."),
                monthly_savings=0,
                annual_savings=0,
                effort='Medium',
                confidence=70.0,
                timeframe='This month'
            ))
            priority += 1

    # 우선순위 정렬 (절감액 기준)
    recs.sort(key=lambda x: x.monthly_savings, reverse=True)
    for i, r in enumerate(recs):
        r.priority = i + 1

    return recs

def format_report(
    recs: List[Recommendation],
    sim: dict,
    quality: dict,
) -> str:
    lines = []
    lines.append("=" * 65)
    lines.append("  InfraLens Optimization Report")
    lines.append("=" * 65)
    lines.append(f"\n  Data:    {quality['clean_rows']:,} rows | "
                 f"{quality['devices']} devices | {quality['date_range']}")
    lines.append(f"  Tier:    {quality['tier']} Analysis\n")

    lines.append("  EXECUTIVE SUMMARY")
    lines.append("  " + "-" * 50)
    lines.append(f"  Current monthly cost:   ${sim['before_monthly']:>10,.2f}")
    lines.append(f"  Optimized monthly cost: ${sim['after_monthly']:>10,.2f}")
    lines.append(f"  Monthly savings:        ${sim['savings_monthly']:>10,.2f}  ({sim['savings_pct']}%)")
    lines.append(f"  Annual savings:         ${sim['savings_annual']:>10,.2f}")
    lines.append(f"  Payback period:         Immediate (no capital required)\n")

    lines.append("  ACTION PLAN (sorted by impact)")
    lines.append("  " + "-" * 50)

    for rec in recs:
        effort_icon = '●' if rec.effort == 'Low' else '◑' if rec.effort == 'Medium' else '○'
        lines.append(f"\n  [{rec.priority}] {rec.title}")
        lines.append(f"      Category:  {rec.category}")
        lines.append(f"      Savings:   ${rec.monthly_savings:,.2f}/mo  (${rec.annual_savings:,.0f}/yr)")
        lines.append(f"      Effort:    {effort_icon} {rec.effort}  |  Timeframe: {rec.timeframe}  |  Confidence: {rec.confidence:.0f}%")
        lines.append(f"      Detail:    {rec.detail}")
        lines.append(f"      Action:    {rec.action}")

    lines.append("\n" + "=" * 65)
    lines.append(f"  TOTAL SAVINGS  ${sim['savings_monthly']:>10,.2f} / month")
    lines.append(f"                 ${sim['savings_annual']:>10,.2f} / year")
    lines.append("=" * 65)

    return "\n".join(lines)

if __name__ == '__main__':
    from data_loader import load_and_prepare
    from cost_model import simulate_before_after
    from analyzer import (detect_idle_advanced, detect_peak_waste_advanced,
                          detect_overprovision_advanced, compute_efficiency_scores)

    df, col_map, quality = load_and_prepare('gpu_metrics_30d.csv')

    idle   = detect_idle_advanced(df)
    peak   = detect_peak_waste_advanced(df)
    over   = detect_overprovision_advanced(df)
    scores = compute_efficiency_scores(df)
    sim    = simulate_before_after(df)

    recs   = generate_recommendations(idle, peak, over, sim, scores)
    report = format_report(recs, sim, quality)

    print(report)
