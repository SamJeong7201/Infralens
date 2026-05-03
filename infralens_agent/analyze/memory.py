"""
memory.py
─────────
역할: GPU 메모리 누수 감지
      util은 낮은데 VRAM이 계속 차는 패턴

알고리즘: 선형회귀 기울기
  - VRAM 사용량의 시간별 기울기 계산
  - GPU util이 낮은데 기울기가 양수면 누수
  - 기울기가 클수록 심각
"""

import pandas as pd
from .anomaly import linear_slope, ema

# 임계값
LEAK_SLOPE_THRESHOLD  = 50.0   # MB/시간 이상 증가하면 누수 의심
LEAK_UTIL_MAX         = 30.0   # GPU util이 이 이하일 때만 (사용 중이면 정상)
LEAK_MEM_MIN_PCT      = 60.0   # VRAM이 이 이상 차있을 때만 알림
LEAK_MIN_HOURS        = 3      # 최소 N시간 데이터 필요


def detect(df: pd.DataFrame) -> list:
    """
    메모리 누수 감지
    반환: alert 리스트
    """
    alerts = []
    if df.empty:
        return alerts

    for gpu_idx in df['gpu_index'].unique():
        gpu_df = df[df['gpu_index'] == gpu_idx].copy()
        gpu_df = gpu_df.sort_values('timestamp')

        if len(gpu_df) < LEAK_MIN_HOURS:
            continue

        latest    = gpu_df.iloc[-1]
        util_ema  = ema(gpu_df['gpu_util'], span=4).iloc[-1]

        # GPU가 실제로 사용 중이면 메모리 증가는 정상
        if util_ema > LEAK_UTIL_MAX:
            continue

        # VRAM 사용량 기울기 계산 (선형회귀)
        mem_slope = linear_slope(gpu_df['mem_used_mb'])

        # 기울기가 임계값 이하면 정상
        if mem_slope < LEAK_SLOPE_THRESHOLD:
            continue

        # 현재 VRAM 사용률
        mem_total = latest.get('mem_total_mb', 1)
        mem_used  = latest.get('mem_used_mb', 0)
        mem_pct   = (mem_used / mem_total * 100) if mem_total > 0 else 0

        # VRAM이 아직 많이 안 찼으면 경고 수준 낮춤
        if mem_pct < LEAK_MEM_MIN_PCT:
            severity = 'medium'
        else:
            severity = 'high'

        # 몇 시간 후 VRAM 꽉 찰지 예측
        remaining_mb = mem_total - mem_used
        hours_to_full = (remaining_mb / mem_slope) if mem_slope > 0 else 999

        alerts.append({
            'type'          : 'MEMORY_LEAK',
            'severity'      : severity,
            'gpu_index'     : int(gpu_idx),
            'gpu_name'      : latest['gpu_name'],
            'util_pct'      : round(float(util_ema), 1),
            'mem_used_mb'   : round(float(mem_used), 0),
            'mem_total_mb'  : round(float(mem_total), 0),
            'mem_pct'       : round(mem_pct, 1),
            'slope_mb_per_h': round(mem_slope, 1),
            'hours_to_full' : round(hours_to_full, 1),
            'message'       : (
                f"GPU-{gpu_idx} possible memory leak: "
                f"VRAM growing +{mem_slope:.0f}MB/h at {util_ema:.0f}% util "
                f"({mem_pct:.0f}% full, OOM in ~{hours_to_full:.0f}h)"
            ),
            'command'       : (
                f"# Find leaking process:\n"
                f"fuser /dev/nvidia{gpu_idx}\n"
                f"nvidia-smi -i {gpu_idx} --query-compute-apps=pid,used_memory --format=csv"
            ),
            'rollback'      : f"# Kill leaking process after identifying PID\nkill -9 <PID>",
            'auto_safe'     : False,  # 프로세스 종료는 수동 확인 필요
        })

    return alerts
