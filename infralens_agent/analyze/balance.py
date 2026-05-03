"""
balance.py
──────────
역할: GPU 간 불균형 사용 감지
      GPU 0만 100% 쓰고 나머지는 놀고 있는 패턴

알고리즘: 지니계수 (Gini Coefficient)
  - 경제학에서 소득 불평등 측정에 쓰는 지표
  - 0 = 완전 균등 사용
  - 1 = 완전 불균등 (한 GPU만 사용)
  - 지니 > 0.4 → 불균형 경고
  - 지니 > 0.6 → 심각한 불균형
"""

import pandas as pd
from .anomaly import gini_coefficient, ema

# 임계값
GINI_WARNING  = 0.4   # 이 이상이면 불균형 경고
GINI_CRITICAL = 0.6   # 이 이상이면 심각
MIN_GPUS      = 2     # GPU가 2개 이상일 때만 의미 있음
MIN_AVG_UTIL  = 10.0  # 클러스터 평균 util이 이 이상일 때만 (완전 idle은 제외)


def detect(df: pd.DataFrame) -> list:
    """
    GPU 불균형 사용 감지
    반환: alert 리스트
    """
    alerts = []
    if df.empty:
        return alerts

    n_gpus = df['gpu_index'].nunique()
    if n_gpus < MIN_GPUS:
        return alerts

    # 최근 데이터만 사용 (최신 타임스탬프)
    latest_ts = df['timestamp'].max()
    latest    = df[df['timestamp'] == latest_ts]

    utils = latest['gpu_util'].values
    avg_util = utils.mean()

    # 클러스터가 거의 안 쓰이면 불균형 감지 의미 없음
    if avg_util < MIN_AVG_UTIL:
        return alerts

    gini = gini_coefficient(list(utils))

    if gini < GINI_WARNING:
        return alerts

    severity = 'critical' if gini >= GINI_CRITICAL else 'medium'

    # 어떤 GPU가 과부하, 어떤 GPU가 놀고 있는지
    overloaded = latest[latest['gpu_util'] > 80][['gpu_index', 'gpu_util']].to_dict('records')
    underused  = latest[latest['gpu_util'] < 20][['gpu_index', 'gpu_util']].to_dict('records')

    # 해결 명령어 (CUDA_VISIBLE_DEVICES 재분배)
    idle_indices = [str(int(g['gpu_index'])) for g in underused]
    command = (
        f"# 워크로드 재분배 — idle GPU 활용:\n"
        f"export CUDA_VISIBLE_DEVICES={','.join(idle_indices)}\n"
        f"# 또는 PyTorch DataParallel 사용:\n"
        f"# model = torch.nn.DataParallel(model, device_ids={idle_indices})"
    ) if idle_indices else "# 스케줄러 설정 확인 필요"

    alerts.append({
        'type'          : 'IMBALANCED_USAGE',
        'severity'      : severity,
        'gini'          : round(gini, 3),
        'avg_util'      : round(float(avg_util), 1),
        'n_overloaded'  : len(overloaded),
        'n_underused'   : len(underused),
        'overloaded_gpus': overloaded,
        'underused_gpus' : underused,
        'message'       : (
            f"GPU imbalance detected (Gini={gini:.2f}): "
            f"{len(overloaded)} GPUs overloaded, "
            f"{len(underused)} GPUs underused at {avg_util:.0f}% avg"
        ),
        'command'       : command,
        'rollback'      : "# unset CUDA_VISIBLE_DEVICES",
        'auto_safe'     : False,
    })

    return alerts
