"""
fake_nvidia_smi.py
──────────────────
현실적인 문제 시나리오 시뮬레이션
시나리오는 시간대별로 자동 순환
"""
import sys
import random
import time
import math

GPUS = [
    (0, 'NVIDIA A100-SXM4-40GB', 400, 40000),
    (1, 'NVIDIA A100-SXM4-40GB', 400, 40000),
    (2, 'NVIDIA A100-SXM4-40GB', 400, 40000),
    (3, 'NVIDIA A100-SXM4-40GB', 400, 40000),
    (4, 'NVIDIA V100-SXM2-32GB', 300, 32000),
    (5, 'NVIDIA V100-SXM2-32GB', 300, 32000),
    (6, 'NVIDIA V100-SXM2-32GB', 300, 32000),
    (7, 'NVIDIA V100-SXM2-32GB', 300, 32000),
]

# 시나리오: 분 단위로 순환 (테스트용)
minute = time.localtime().tm_min
scenario = (minute // 10) % 6  # 10분마다 시나리오 바뀜

def get_gpu_state(idx, name, tdp, mem_total):
    """시나리오별 GPU 상태 반환"""

    # 시나리오 0: 정상 (baseline)
    if scenario == 0:
        util     = random.uniform(60, 85)
        mem_used = int(mem_total * random.uniform(0.5, 0.8))
        power    = tdp * (0.3 + util / 100 * 0.7)
        temp     = 45 + util * 0.4

    # 시나리오 1: 다수 GPU idle (전력 낭비)
    elif scenario == 1:
        if idx in [2, 3, 4, 5]:
            util     = random.uniform(0, 4)       # idle
            mem_used = int(mem_total * 0.02)
            power    = tdp * 0.15
            temp     = 32 + random.uniform(-2, 2)
        else:
            util     = random.uniform(70, 95)
            mem_used = int(mem_total * 0.7)
            power    = tdp * (0.3 + util / 100 * 0.7)
            temp     = 50 + util * 0.35

    # 시나리오 2: 좀비 프로세스 (util=0, VRAM 점유)
    elif scenario == 2:
        if idx in [1, 3]:
            util     = random.uniform(0, 2)       # 거의 0
            mem_used = int(mem_total * 0.75)      # VRAM 꽉 참
            power    = tdp * 0.18
            temp     = 35 + random.uniform(-2, 2)
        else:
            util     = random.uniform(40, 75)
            mem_used = int(mem_total * 0.5)
            power    = tdp * (0.3 + util / 100 * 0.7)
            temp     = 45 + util * 0.35

    # 시나리오 3: 불균형 (GPU 0만 100%, 나머지 놀음)
    elif scenario == 3:
        if idx == 0:
            util     = random.uniform(92, 100)
            mem_used = int(mem_total * 0.95)
            power    = tdp * 0.98
            temp     = 80 + random.uniform(-2, 2)
        elif idx == 7:
            util     = random.uniform(85, 95)
            mem_used = int(mem_total * 0.85)
            power    = tdp * 0.9
            temp     = 75 + random.uniform(-2, 2)
        else:
            util     = random.uniform(2, 10)      # 나머지 놀음
            mem_used = int(mem_total * 0.05)
            power    = tdp * 0.16
            temp     = 33 + random.uniform(-2, 2)

    # 시나리오 4: 과열 (온도 위험)
    elif scenario == 4:
        if idx in [0, 1]:
            util     = random.uniform(95, 100)
            mem_used = int(mem_total * 0.9)
            power    = tdp * 0.99
            temp     = 88 + random.uniform(0, 5)  # 과열!
        else:
            util     = random.uniform(50, 75)
            mem_used = int(mem_total * 0.5)
            power    = tdp * (0.3 + util / 100 * 0.7)
            temp     = 55 + util * 0.3

    # 시나리오 5: 오버프로비저닝 (항상 낮은 util)
    else:
        util     = random.uniform(8, 22)
        mem_used = int(mem_total * random.uniform(0.1, 0.25))
        power    = tdp * (0.2 + util / 100 * 0.5)
        temp     = 38 + util * 0.3

    mem_util = mem_used / mem_total * 100
    fan      = min(100, util * 0.75 + 20)
    return util, mem_util, mem_used, mem_total, power, tdp, temp, fan


args = ' '.join(sys.argv[1:])

SCENARIO_NAMES = [
    'NORMAL', 'IDLE_WASTE', 'ZOMBIE',
    'IMBALANCED', 'THERMAL', 'OVERPROVISION'
]
print(f'# [SCENARIO {scenario}: {SCENARIO_NAMES[scenario]}]', file=sys.stderr)

if '--query-gpu' in args and '--format=csv' in args:
    for idx, name, tdp, mem_total in GPUS:
        util, mem_util, mem_used, mem_total, power, limit, temp, fan = get_gpu_state(idx, name, tdp, mem_total)
        print(f'{idx}, {name}, {util:.1f}, {mem_util:.1f}, '
              f'{mem_used}, {mem_total}, {power:.1f}, {limit}, '
              f'{temp:.1f}, {fan:.0f}')

elif '-pl' in args:
    print('Power limit constraint set to successfully.')

else:
    print('NVIDIA-SMI 525.85.12   Driver Version: 525.85.12   CUDA Version: 12.0')
    for idx, name, tdp, mem_total in GPUS:
        util, *_ = get_gpu_state(idx, name, tdp, mem_total)
        print(f'GPU {idx}: {name} (util: {util:.0f}%)')
