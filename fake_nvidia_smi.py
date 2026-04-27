"""
fake_nvidia_smi.py
──────────────────
실제 nvidia-smi 없이 테스트용 가짜 데이터 생성
collect.py가 이걸 실제처럼 사용
"""
import sys
import random
import time

# 가짜 GPU 설정
GPUS = [
    (0, 'NVIDIA A100-SXM4-40GB', 400),
    (1, 'NVIDIA A100-SXM4-40GB', 400),
    (2, 'NVIDIA A100-SXM4-40GB', 400),
    (3, 'NVIDIA A100-SXM4-40GB', 400),
    (4, 'NVIDIA V100-SXM2-32GB', 300),
    (5, 'NVIDIA V100-SXM2-32GB', 300),
    (6, 'NVIDIA V100-SXM2-32GB', 300),
    (7, 'NVIDIA V100-SXM2-32GB', 300),
]

# 시간대별 패턴 (현실적)
hour = time.localtime().tm_hour
if 9 <= hour <= 18:
    base_util = random.uniform(40, 80)   # 업무시간
elif 19 <= hour <= 22:
    base_util = random.uniform(20, 50)   # 저녁
else:
    base_util = random.uniform(2, 15)    # 야간 (idle)

args = ' '.join(sys.argv[1:])

if '--query-gpu' in args and '--format=csv' in args:
    # nvidia-smi --query-gpu=... --format=csv,noheader,nounits
    for idx, name, tdp in GPUS:
        # GPU마다 다른 패턴
        if idx in [2, 3, 6]:  # 일부 GPU는 항상 idle
            util = random.uniform(1, 8)
        else:
            util = max(0, min(100, base_util + random.uniform(-15, 15)))
        
        mem_util = util * 0.7 + random.uniform(-5, 5)
        mem_used = int(40000 * mem_util / 100)
        mem_total = 40000
        power = tdp * (0.15 + util/100 * 0.85)
        temp = 30 + util * 0.55 + random.uniform(-3, 3)
        fan = min(100, util * 0.8 + 20)
        
        print(f'{idx}, {name}, {util:.1f}, {mem_util:.1f}, '
              f'{mem_used}, {mem_total}, {power:.1f}, {tdp}, '
              f'{temp:.1f}, {fan:.0f}')

elif '--query-gpu' in args and 'power.draw' in args:
    for idx, name, tdp in GPUS:
        util = random.uniform(2, 80)
        power = tdp * (0.15 + util/100 * 0.85)
        limit = tdp
        print(f'{idx}, {name}, {power:.1f}, {limit}')

elif '-pl' in args:
    # nvidia-smi -i 0 -pl 75
    print('Power limit constraint set to: 75 W')

elif '-mig' in args:
    print('MIG mode enabled')

else:
    # 기본 출력
    print('NVIDIA-SMI 525.85.12   Driver Version: 525.85.12   CUDA Version: 12.0')
    for idx, name, tdp in GPUS:
        util = random.uniform(2, 80)
        print(f'GPU {idx}: {name} (util: {util:.0f}%)')
