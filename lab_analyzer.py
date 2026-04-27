"""
lab_analyzer.py
───────────────
역할: 연구실 GPU 데이터 분석
      비즈니스 버전과 완전히 분리
"""
import pandas as pd
import numpy as np
from dataclasses import dataclass
from typing import List, Dict, Optional


@dataclass
class LabInsight:
    category: str        # 분석 카테고리
    title: str           # 제목
    detail: str          # 설명
    metric: float        # 핵심 수치
    metric_label: str    # 수치 단위/설명
    severity: str        # 'critical' / 'warning' / 'info'
    affected: str        # 영향받는 대상 (user/gpu/job)


# ══════════════════════════════════════════
# 1. 사용자별 공정성 분석
# ══════════════════════════════════════════

def analyze_user_fairness(metrics_df: pd.DataFrame,
                          jobs_df: Optional[pd.DataFrame] = None) -> Dict:
    """
    누가 GPU를 얼마나 쓰는지 분석
    - 사용자별 GPU 시간
    - 역할별 분포
    - 독점 사용자 감지
    """
    if 'user' not in metrics_df.columns:
        return {}

    total_rows = len(metrics_df)
    active = metrics_df[metrics_df['gpu_util'] >= 15]

    # 사용자별 GPU 시간 (시간 단위)
    user_hours = active.groupby('user').size() / 1  # 1행 = 1시간
    user_pct   = (user_hours / total_rows * 100).round(1)

    # 역할별 집계
    role_summary = {}
    if 'user_role' in metrics_df.columns:
        role_hours = active.groupby('user_role').size()
        role_pct   = (role_hours / total_rows * 100).round(1)
        role_summary = role_pct.to_dict()

    # 상위 사용자
    top_users = user_pct.sort_values(ascending=False).head(10)

    # 독점 감지 (상위 1명이 30% 이상)
    monopoly_user = None
    monopoly_pct  = 0
    if len(top_users) > 0:
        monopoly_pct  = float(top_users.iloc[0])
        monopoly_user = top_users.index[0]

    # 대기 시간 분석 (Slurm 데이터 있으면)
    wait_by_user = {}
    if jobs_df is not None and 'wait_minutes' in jobs_df.columns:
        wait_by_user = jobs_df.groupby('user')['wait_minutes'].mean().round(0).to_dict()

    return {
        'user_gpu_pct':    user_pct.to_dict(),
        'user_gpu_hours':  user_hours.to_dict(),
        'role_summary':    role_summary,
        'top_users':       top_users.to_dict(),
        'monopoly_user':   monopoly_user,
        'monopoly_pct':    monopoly_pct,
        'wait_by_user':    wait_by_user,
        'total_users':     metrics_df['user'].nunique(),
    }


# ══════════════════════════════════════════
# 2. Job 효율성 분석
# ══════════════════════════════════════════

def analyze_job_efficiency(metrics_df: pd.DataFrame,
                           jobs_df: Optional[pd.DataFrame] = None) -> Dict:
    """
    어떤 job이 GPU를 낭비하는지 분석
    - Job 타입별 평균 사용률
    - 낮은 효율 job 감지
    - Interactive job 과다 사용 감지
    """
    results = {}

    # metrics에서 job_type 분석
    if 'job_type' in metrics_df.columns:
        job_util = metrics_df.groupby('job_type').agg(
            avg_util=('gpu_util', 'mean'),
            total_hours=('gpu_util', 'count'),
            pct_of_total=('gpu_util', lambda x: len(x) / len(metrics_df) * 100)
        ).round(1)

        # 낭비 job (평균 사용률 < 30%)
        wasteful_jobs = job_util[job_util['avg_util'] < 30].sort_values('avg_util')
        results['job_type_summary'] = job_util.to_dict()
        results['wasteful_jobs']    = wasteful_jobs.to_dict()

        # Interactive job 비율
        interactive_rows = metrics_df[
            metrics_df['job_type'].str.contains('interactive|test|debug', case=False, na=False)
        ]
        results['interactive_pct'] = round(len(interactive_rows) / len(metrics_df) * 100, 1)

    # Slurm job 분석
    if jobs_df is not None:
        # 평균 대기 시간
        results['avg_wait_minutes']  = round(jobs_df['wait_minutes'].mean(), 0)
        results['max_wait_minutes']  = round(jobs_df['wait_minutes'].max(), 0)
        results['median_wait']       = round(jobs_df['wait_minutes'].median(), 0)

        # 실패한 job
        if 'exit_code' in jobs_df.columns:
            failed = jobs_df[jobs_df['exit_code'] != 0]
            results['failed_job_pct'] = round(len(failed) / len(jobs_df) * 100, 1)
            results['failed_jobs']    = len(failed)

        # 낮은 효율 job (GPU 여러 개 요청했는데 실제 사용률 낮음)
        if 'avg_gpu_util' in jobs_df.columns and 'gpu_count' in jobs_df.columns:
            multi_gpu = jobs_df[jobs_df['gpu_count'] >= 2]
            low_eff   = multi_gpu[multi_gpu['avg_gpu_util'] < 30]
            results['multi_gpu_waste_pct'] = round(
                len(low_eff) / max(len(multi_gpu), 1) * 100, 1
            )
            results['multi_gpu_waste_jobs'] = len(low_eff)

        # 파티션별 대기 시간
        if 'partition' in jobs_df.columns:
            results['wait_by_partition'] = jobs_df.groupby('partition')['wait_minutes'].mean().round(0).to_dict()

        # 사용자별 job 수
        results['jobs_by_user'] = jobs_df.groupby('user').size().to_dict()
        results['jobs_by_type'] = jobs_df.groupby('job_name').size().to_dict() if 'job_name' in jobs_df.columns else {}

    return results


# ══════════════════════════════════════════
# 3. 전력 및 열 분석
# ══════════════════════════════════════════

def analyze_power_thermal(metrics_df: pd.DataFrame) -> Dict:
    """
    연구실 전력 비용 분석
    - 총 전력 소비
    - GPU별 전력 효율
    - 열 문제 감지
    """
    results = {}

    if 'power_draw' not in metrics_df.columns:
        return results

    # 총 전력 (kWh/월) - GPU 수 * 시간당 평균 전력
    n_gpus_power = metrics_df['gpu_id'].nunique() if 'gpu_id' in metrics_df.columns else 1
    avg_power_w  = metrics_df['power_draw'].mean()
    # 실제 총 전력 = GPU 수 * 평균 전력 * 시간
    total_kwh    = avg_power_w * n_gpus_power * 24 * 30 / 1000
    # 연구실 전기세 $0.12/kWh (대학 캠퍼스 평균)
    electricity_cost = total_kwh * 0.12
    results['avg_power_w']       = round(avg_power_w, 1)
    results['monthly_kwh']       = round(total_kwh, 0)
    results['monthly_elec_cost'] = round(electricity_cost, 0)
    results['n_gpus_power']      = n_gpus_power

    # idle 전력 낭비
    idle = metrics_df[metrics_df['gpu_util'] < 15]
    idle_power_w    = idle['power_draw'].mean() if len(idle) > 0 else 0
    idle_kwh        = idle_power_w * n_gpus_power * 24 * 30 / 1000
    idle_cost       = idle_kwh * 0.12
    results['idle_power_w']    = round(idle_power_w, 1)
    results['idle_kwh']        = round(idle_kwh, 0)
    results['idle_elec_cost']  = round(idle_cost, 0)
    results['idle_pct']        = round(len(idle) / len(metrics_df) * 100, 1)

    # GPU별 효율
    if 'gpu_id' in metrics_df.columns:
        gpu_eff = metrics_df.groupby('gpu_id').agg(
            avg_util=('gpu_util', 'mean'),
            avg_power=('power_draw', 'mean'),
        ).round(1)
        gpu_eff['perf_per_watt'] = (gpu_eff['avg_util'] / gpu_eff['avg_power']).round(3)
        results['gpu_efficiency'] = gpu_eff.to_dict()

    # 열 문제
    if 'temperature' in metrics_df.columns:
        hot_gpus = metrics_df[metrics_df['temperature'] > 85]
        results['thermal_warning_pct'] = round(len(hot_gpus) / len(metrics_df) * 100, 1)
        results['max_temp'] = round(metrics_df['temperature'].max(), 1)
        results['avg_temp'] = round(metrics_df['temperature'].mean(), 1)

    return results


# ══════════════════════════════════════════
# 4. 클러스터 활용률 분석
# ══════════════════════════════════════════

def analyze_cluster_utilization(metrics_df: pd.DataFrame,
                                jobs_df: Optional[pd.DataFrame] = None) -> Dict:
    """
    클러스터 전체 활용률 분석
    - 시간대별 패턴
    - 주말 vs 평일
    - GPU 타입별 활용률
    - 병목 감지
    """
    results = {}

    # 전체 평균 사용률
    results['overall_util']  = round(metrics_df['gpu_util'].mean(), 1)
    results['peak_util']     = round(metrics_df['gpu_util'].max(), 1)
    results['idle_util_pct'] = round(
        (metrics_df['gpu_util'] < 15).sum() / len(metrics_df) * 100, 1
    )

    # 시간대별 패턴
    if 'hour' in metrics_df.columns:
        hourly = metrics_df.groupby('hour')['gpu_util'].mean().round(1)
        results['hourly_util']   = hourly.to_dict()
        results['peak_hour']     = int(hourly.idxmax())
        results['lowest_hour']   = int(hourly.idxmin())
        results['overnight_util'] = round(
            metrics_df[metrics_df['hour'].isin(range(23,24)) | metrics_df['hour'].isin(range(0,8))]['gpu_util'].mean(), 1
        )

    # 주말 vs 평일
    if 'is_weekend' in metrics_df.columns:
        weekend_util  = metrics_df[metrics_df['is_weekend']]['gpu_util'].mean()
        weekday_util  = metrics_df[~metrics_df['is_weekend']]['gpu_util'].mean()
        results['weekend_util']  = round(weekend_util, 1)
        results['weekday_util']  = round(weekday_util, 1)
        results['weekend_waste'] = round((weekday_util - weekend_util), 1)

    # GPU 모델별 활용률
    if 'gpu_model' in metrics_df.columns:
        model_util = metrics_df.groupby('gpu_model')['gpu_util'].mean().round(1)
        results['model_util'] = model_util.to_dict()

    # 파티션별
    if 'partition' in metrics_df.columns:
        partition_util = metrics_df.groupby('partition')['gpu_util'].mean().round(1)
        results['partition_util'] = partition_util.to_dict()

    # 총 GPU 시간
    n_gpus   = metrics_df['gpu_id'].nunique() if 'gpu_id' in metrics_df.columns else 1
    total_h  = len(metrics_df) / n_gpus
    used_h   = len(metrics_df[metrics_df['gpu_util'] >= 15]) / n_gpus
    wasted_h = total_h - used_h
    results['total_gpu_hours']  = round(total_h, 0)
    results['used_gpu_hours']   = round(used_h, 0)
    results['wasted_gpu_hours'] = round(wasted_h, 0)
    results['n_gpus']           = n_gpus

    return results


# ══════════════════════════════════════════
# 5. 큐 병목 분석
# ══════════════════════════════════════════

def analyze_queue_bottleneck(jobs_df: pd.DataFrame) -> Dict:
    """
    왜 대기 시간이 긴지 분석
    - 피크 시간대 병목
    - 장시간 점유 job
    - GPU 수 요청 패턴
    """
    if jobs_df is None or 'wait_minutes' not in jobs_df.columns:
        return {}

    results = {}

    # 대기 시간 분포
    results['avg_wait']    = round(jobs_df['wait_minutes'].mean(), 0)
    results['p90_wait']    = round(jobs_df['wait_minutes'].quantile(0.9), 0)
    results['p99_wait']    = round(jobs_df['wait_minutes'].quantile(0.99), 0)
    results['long_wait_jobs'] = int((jobs_df['wait_minutes'] > 120).sum())

    # GPU 수 요청 패턴
    if 'gpu_count' in jobs_df.columns:
        gpu_demand = jobs_df['gpu_count'].value_counts().to_dict()
        results['gpu_demand'] = gpu_demand
        results['avg_gpu_request'] = round(jobs_df['gpu_count'].mean(), 1)
        results['multi_gpu_pct'] = round(
            (jobs_df['gpu_count'] >= 2).sum() / len(jobs_df) * 100, 1
        )

    # 장시간 점유 job
    if 'run_minutes' in jobs_df.columns:
        long_jobs = jobs_df[jobs_df['run_minutes'] > 360]  # 6시간 이상
        results['long_job_pct'] = round(len(long_jobs) / len(jobs_df) * 100, 1)
        results['long_job_count'] = len(long_jobs)
        results['avg_run_hours'] = round(jobs_df['run_minutes'].mean() / 60, 1)

    # 사용자별 대기 시간
    if 'user' in jobs_df.columns:
        results['wait_by_user'] = jobs_df.groupby('user')['wait_minutes'].mean().round(0).to_dict()

    return results


# ══════════════════════════════════════════
# 메인 분석 함수
# ══════════════════════════════════════════

def run_lab_analysis(metrics_df: pd.DataFrame,
                     jobs_df: Optional[pd.DataFrame] = None) -> Dict:
    """
    전체 연구실 분석 실행
    """
    print('Running lab analysis...')

    results = {
        'user_fairness':     analyze_user_fairness(metrics_df, jobs_df),
        'job_efficiency':    analyze_job_efficiency(metrics_df, jobs_df),
        'power_thermal':     analyze_power_thermal(metrics_df),
        'cluster_util':      analyze_cluster_utilization(metrics_df, jobs_df),
        'queue_bottleneck':  analyze_queue_bottleneck(jobs_df) if jobs_df is not None else {},
        'n_rows':            len(metrics_df),
        'n_gpus':            metrics_df['gpu_id'].nunique() if 'gpu_id' in metrics_df.columns else 0,
        'n_users':           metrics_df['user'].nunique() if 'user' in metrics_df.columns else 0,
        'date_range':        f"{metrics_df['date'].min()} to {metrics_df['date'].max()}" if 'date' in metrics_df.columns else 'N/A',
    }

    print('Done.')
    return results


if __name__ == '__main__':
    metrics = pd.read_csv('lab_gpu_metrics.csv')
    jobs    = pd.read_csv('lab_slurm_jobs.csv')

    results = run_lab_analysis(metrics, jobs)

    cu = results['cluster_util']
    pt = results['power_thermal']
    uf = results['user_fairness']
    je = results['job_efficiency']
    qb = results['queue_bottleneck']

    print(f'\n=== CLUSTER OVERVIEW ===')
    print(f'GPUs: {results["n_gpus"]}  |  Users: {results["n_users"]}')
    print(f'Overall util:    {cu["overall_util"]}%')
    print(f'Idle time:       {cu["idle_util_pct"]}%')
    print(f'Weekend util:    {cu["weekend_util"]}%')
    print(f'Weekday util:    {cu["weekday_util"]}%')
    print(f'Overnight util:  {cu["overnight_util"]}%')

    print(f'\n=== POWER ===')
    print(f'Monthly kWh:     {pt["monthly_kwh"]:,.0f}')
    print(f'Monthly cost:    ${pt["monthly_elec_cost"]:,.0f}')
    print(f'Idle cost:       ${pt["idle_elec_cost"]:,.0f}')

    print(f'\n=== USERS ===')
    print(f'Top user:        {uf["monopoly_user"]} ({uf["monopoly_pct"]}%)')
    for user, pct in sorted(uf['user_gpu_pct'].items(), key=lambda x: -x[1])[:5]:
        print(f'  {user}: {pct}%')

    print(f'\n=== QUEUE ===')
    print(f'Avg wait:        {qb.get("avg_wait", 0):.0f} min')
    print(f'P90 wait:        {qb.get("p90_wait", 0):.0f} min')
    print(f'Long wait jobs:  {qb.get("long_wait_jobs", 0)}')

    print(f'\n=== JOB EFFICIENCY ===')
    print(f'Interactive %:   {je.get("interactive_pct", 0)}%')
    print(f'Failed jobs:     {je.get("failed_jobs", 0)}')
    print(f'Avg wait:        {je.get("avg_wait_minutes", 0):.0f} min')
