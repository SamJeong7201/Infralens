"""
env_detect.py
─────────────
역할: 서버 환경 자동 감지
      AWS / GCP / Azure / On-prem / Slurm
      감지 결과에 따라 수집/명령어 자동 조정

알고리즘: 각 환경별 고유 신호 확인
"""
import subprocess
import urllib.request
import os
from pathlib import Path


def _run(cmd: list, timeout=3) -> str:
    """명령어 실행, 실패하면 빈 문자열"""
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        return r.stdout.strip()
    except:
        return ''


def _fetch(url: str, timeout=2) -> str:
    """URL 가져오기, 실패하면 빈 문자열"""
    try:
        req = urllib.request.Request(url, headers={'Metadata': 'true'})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.read().decode()
    except:
        return ''


def detect_cloud() -> dict:
    """
    클라우드 환경 감지
    각 클라우드는 고유한 메타데이터 엔드포인트가 있음
    """

    # AWS — IMDSv2
    aws_token = _fetch('http://169.254.169.254/latest/api/token')
    if aws_token:
        instance_type = _fetch('http://169.254.169.254/latest/meta-data/instance-type')
        region        = _fetch('http://169.254.169.254/latest/meta-data/placement/region')
        return {
            'cloud'        : 'AWS',
            'instance_type': instance_type,
            'region'       : region,
        }

    # GCP — metadata server
    gcp = _fetch('http://metadata.google.internal/computeMetadata/v1/instance/machine-type')
    if gcp:
        zone = _fetch('http://metadata.google.internal/computeMetadata/v1/instance/zone')
        return {
            'cloud'        : 'GCP',
            'instance_type': gcp.split('/')[-1],
            'region'       : zone.split('/')[-1] if zone else '',
        }

    # Azure — IMDS
    azure = _fetch('http://169.254.169.254/metadata/instance?api-version=2021-02-01')
    if azure:
        return {
            'cloud'        : 'Azure',
            'instance_type': '',
            'region'       : '',
        }

    return {'cloud': 'on-prem', 'instance_type': '', 'region': ''}


def detect_scheduler() -> dict:
    """job 스케줄러 감지 (Slurm / PBS / LSF)"""

    # Slurm
    if _run(['which', 'squeue']):
        version = _run(['sinfo', '--version'])
        nodes   = _run(['sinfo', '--noheader', '-o', '%N'])
        return {
            'scheduler': 'slurm',
            'version'  : version,
            'nodes'    : nodes,
        }

    # PBS/Torque
    if _run(['which', 'qstat']):
        return {'scheduler': 'pbs', 'version': '', 'nodes': ''}

    # LSF
    if _run(['which', 'bjobs']):
        return {'scheduler': 'lsf', 'version': '', 'nodes': ''}

    return {'scheduler': 'none', 'version': '', 'nodes': ''}


def detect_gpu() -> dict:
    """GPU 환경 감지"""

    # nvidia-smi
    nvidia = _run(['nvidia-smi', '--query-gpu=name,driver_version,memory.total',
                   '--format=csv,noheader'])
    if nvidia:
        gpus = []
        for line in nvidia.strip().split('\n'):
            parts = [p.strip() for p in line.split(',')]
            if len(parts) >= 3:
                gpus.append({
                    'name'          : parts[0],
                    'driver_version': parts[1],
                    'memory_mb'     : parts[2],
                })
        return {'vendor': 'nvidia', 'gpus': gpus, 'count': len(gpus)}

    # AMD ROCm
    if _run(['which', 'rocm-smi']):
        return {'vendor': 'amd', 'gpus': [], 'count': 0}

    return {'vendor': 'none', 'gpus': [], 'count': 0}


def detect_os() -> dict:
    """OS 환경 감지"""
    os_info = _run(['uname', '-a'])
    cpu     = _run(['nproc'])
    mem     = ''

    # 메모리 (Linux)
    if Path('/proc/meminfo').exists():
        try:
            meminfo = Path('/proc/meminfo').read_text()
            for line in meminfo.split('\n'):
                if 'MemTotal' in line:
                    mem = line.split(':')[1].strip()
                    break
        except:
            pass

    return {
        'os'     : 'linux' if 'Linux' in os_info else 'mac' if 'Darwin' in os_info else 'other',
        'cpu_cores': cpu,
        'memory' : mem,
    }


def detect_all() -> dict:
    """
    전체 환경 감지
    반환: 환경 정보 딕셔너리
    """
    cloud     = detect_cloud()
    scheduler = detect_scheduler()
    gpu       = detect_gpu()
    os_info   = detect_os()

    env = {
        'cloud'    : cloud,
        'scheduler': scheduler,
        'gpu'      : gpu,
        'os'       : os_info,
    }

    # 환경별 권장 설정
    env['recommendations'] = _get_env_recommendations(env)

    return env


def _get_env_recommendations(env: dict) -> list:
    """환경에 맞는 권장 설정"""
    recs = []
    cloud     = env['cloud']['cloud']
    scheduler = env['scheduler']['scheduler']

    if cloud == 'AWS':
        recs.append({
            'type'   : 'cost_tool',
            'message': 'AWS Cost Explorer로 GPU 비용 추적 가능',
            'command': 'aws ce get-cost-and-usage --time-period ...'
        })
        instance = env['cloud'].get('instance_type', '')
        if instance and 'p4' in instance:
            recs.append({
                'type'   : 'instance_upgrade',
                'message': f'{instance} — A100 인스턴스, MIG 분할 고려',
                'command': 'nvidia-smi mig -lgip'
            })

    elif cloud == 'GCP':
        recs.append({
            'type'   : 'cost_tool',
            'message': 'GCP Billing으로 GPU 비용 추적 가능',
            'command': 'gcloud billing accounts list'
        })

    if scheduler == 'slurm':
        recs.append({
            'type'   : 'scheduler',
            'message': 'Slurm 감지됨 — GPU 할당 효율 분석 가능',
            'command': 'squeue --format="%i %u %j %R %G" --noheader'
        })

    return recs


def print_env(env: dict):
    """환경 정보 출력"""
    cloud = env['cloud']
    sched = env['scheduler']
    gpu   = env['gpu']
    os_i  = env['os']

    print('🔍 Environment Detection:')
    print(f"  Cloud:     {cloud['cloud']}"
          + (f" ({cloud['instance_type']})" if cloud.get('instance_type') else ''))
    print(f"  Scheduler: {sched['scheduler']}")
    print(f"  GPU:       {gpu['vendor']} x{gpu['count']}")
    print(f"  OS:        {os_i['os']} ({os_i['cpu_cores']} cores)")

    if env.get('recommendations'):
        print(f"  💡 {len(env['recommendations'])} env-specific recommendations")


if __name__ == '__main__':
    env = detect_all()
    print_env(env)
    import json
    print(json.dumps(env, indent=2))
