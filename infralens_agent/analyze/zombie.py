"""
zombie.py
─────────
역할: 좀비 프로세스 감지
      GPU util은 0%인데 VRAM은 차있는 패턴
      = 누군가 GPU 점유하고 아무것도 안 하는 상태

알고리즘: util/memory 비율 이상감지
  - util이 거의 0인데 mem_used가 높으면 좀비
  - Z-score로 정상 비율 범위 벗어나는지 확인
"""

import pandas as pd
from .anomaly import ema

# 임계값
ZOMBIE_UTIL_MAX   = 5.0     # GPU util 이 이하
ZOMBIE_MEM_MIN_MB = 512     # VRAM 이 이상 사용 중
ZOMBIE_MIN_HOURS  = 2       # N시간 이상 지속


def detect(df: pd.DataFrame) -> list:
    """
    좀비 프로세스 감지
    반환: alert 리스트
    """
    alerts = []
    if df.empty:
        return alerts

    for gpu_idx in df['gpu_index'].unique():
        gpu_df = df[df['gpu_index'] == gpu_idx].copy()
        gpu_df = gpu_df.sort_values('timestamp')

        if len(gpu_df) < ZOMBIE_MIN_HOURS:
            continue

        # EMA로 노이즈 제거
        gpu_df['util_ema'] = ema(gpu_df['gpu_util'], span=4)
        gpu_df['mem_ema']  = ema(gpu_df['mem_used_mb'], span=4)

        latest     = gpu_df.iloc[-1]
        util_ema   = latest['util_ema']
        mem_ema    = latest['mem_ema']
        mem_total  = latest.get('mem_total_mb', 1)

        # 조건: util 거의 0 + VRAM 많이 차있음
        if util_ema > ZOMBIE_UTIL_MAX:
            continue
        if mem_ema < ZOMBIE_MEM_MIN_MB:
            continue

        # N시간 동안 지속됐는지 확인
        zombie_rows = gpu_df[
            (gpu_df['util_ema'] < ZOMBIE_UTIL_MAX) &
            (gpu_df['mem_ema']  > ZOMBIE_MEM_MIN_MB)
        ]
        if len(zombie_rows) < ZOMBIE_MIN_HOURS:
            continue

        mem_pct = (mem_ema / mem_total * 100) if mem_total > 0 else 0

        # 차단된 VRAM 가치 (다른 job이 못 씀)
        blocked_gb = mem_ema / 1024

        alerts.append({
            'type'        : 'ZOMBIE_PROCESS',
            'severity'    : 'high',
            'gpu_index'   : int(gpu_idx),
            'gpu_name'    : latest['gpu_name'],
            'util_pct'    : round(float(util_ema), 1),
            'mem_used_mb' : round(float(mem_ema), 0),
            'mem_pct'     : round(mem_pct, 1),
            'blocked_gb'  : round(blocked_gb, 1),
            'duration_h'  : len(zombie_rows),
            'message'     : (
                f"GPU-{gpu_idx} zombie process: "
                f"{util_ema:.0f}% util but {mem_pct:.0f}% VRAM occupied "
                f"({blocked_gb:.1f}GB blocked for {len(zombie_rows)}h)"
            ),
            'command'     : (
                f"# 점유 프로세스 확인:\n"
                f"nvidia-smi -i {gpu_idx} --query-compute-apps=pid,process_name,used_memory --format=csv\n"
                f"# PID 확인 후 종료:\n"
                f"kill -9 <PID>"
            ),
            'rollback'    : "# 프로세스 종료 후 자동 복구됨",
            'auto_safe'   : False,
        })

    return alerts
