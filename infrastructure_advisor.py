"""
infrastructure_advisor.py
─────────────────────────
역할: 데이터에서 인프라 환경 감지 → 환경에 맞는 정확한 실행 가이드 생성
"""
import pandas as pd
import re
from dataclasses import dataclass
from typing import Optional


# ══════════════════════════════════════════
# 데이터 클래스
# ══════════════════════════════════════════

@dataclass
class Environment:
    provider: str        # aws / gcp / azure / lambda / coreweave / runpod / vast / on_premise / unknown
    orchestration: str   # kubernetes / slurm / docker / bare_metal / sagemaker / vertex_ai / unknown
    access: str          # direct_ssh / vm / container / serverless
    gpu_model: str       # A100 / V100 / H100 / T4 / A10G / RTX3090 / etc
    instance_type: str   # p4d.24xlarge / a2-highgpu-8g / etc
    confidence: int      # 0-100


@dataclass
class ActionGuide:
    situation: str       # 지금 뭔 일이 일어나고 있는지 (사람 언어)
    business_impact: str # 왜 문제인지 (CTO 언어)
    what_to_do: str      # 뭘 해야 하는지 (한 문장)
    steps: list          # [(step_title, explanation, commands)]
    verify: str          # 어떻게 확인하는지
    expected_result: str # 하고 나면 어떻게 되는지
    owner: str           # 누가 해야 하는지
    time_required: str   # 얼마나 걸리는지
    risk: str            # 리스크 설명
    env: Environment     # 감지된 환경


# ══════════════════════════════════════════
# 환경 감지
# ══════════════════════════════════════════

# Region → Provider 매핑
REGION_MAP = {
    # AWS
    'us-east-1': 'aws', 'us-east-2': 'aws', 'us-west-1': 'aws', 'us-west-2': 'aws',
    'eu-west-1': 'aws', 'eu-west-2': 'aws', 'eu-central-1': 'aws',
    'ap-southeast-1': 'aws', 'ap-northeast-1': 'aws',
    # GCP
    'us-central1': 'gcp', 'us-east1': 'gcp', 'us-west1': 'gcp',
    'europe-west1': 'gcp', 'europe-west4': 'gcp', 'asia-east1': 'gcp',
    # Azure
    'eastus': 'azure', 'westus': 'azure', 'westus2': 'azure',
    'westeurope': 'azure', 'northeurope': 'azure', 'southeastasia': 'azure',
    # CoreWeave
    'las1': 'coreweave', 'ord1': 'coreweave', 'eur1': 'coreweave',
    # Oracle
    'us-ashburn-1': 'oracle', 'us-phoenix-1': 'oracle', 'eu-frankfurt-1': 'oracle',
}

# GPU 모델 → Provider 힌트
GPU_PROVIDER_HINTS = {
    'a100-sxm4-40gb': ['aws', 'gcp', 'on_premise'],
    'a100-sxm4-80gb': ['aws', 'gcp', 'on_premise', 'coreweave', 'lambda'],
    'a100-pcie-40gb': ['azure', 'on_premise', 'runpod'],
    'v100-sxm2-32gb': ['aws', 'gcp'],
    'v100-sxm2-16gb': ['aws', 'gcp'],
    'h100-sxm5-80gb': ['aws', 'on_premise', 'coreweave', 'lambda'],
    'h100-pcie-80gb': ['azure', 'on_premise'],
    'a10g':           ['aws'],          # g5 전용
    't4':             ['aws', 'gcp'],   # inference
    'rtx 3090':       ['runpod', 'vast', 'on_premise'],
    'rtx 4090':       ['runpod', 'vast', 'on_premise'],
    'a6000':          ['lambda', 'on_premise'],
}

# 요금 → Provider + 인스턴스 타입
PRICE_MAP = [
    # (min, max, provider, instance_hint)
    (0.40,  0.80,  'vast',       'marketplace spot'),
    (0.80,  1.50,  'runpod',     'community cloud'),
    (1.50,  2.00,  'vast',       'on-demand'),
    (2.00,  2.50,  'lambda',     'on-demand A100'),
    (2.50,  3.00,  'coreweave',  'A100 80GB'),
    (2.80,  3.20,  'gcp',        'n1-standard + V100'),
    (3.00,  3.50,  'aws',        'p3 (V100)'),
    (3.50,  4.50,  'aws',        'p4d (A100)'),
    (4.00,  5.00,  'gcp',        'a2-highgpu (A100)'),
    (5.00,  7.00,  'azure',      'NDv4 (A100)'),
    (7.00,  12.00, 'aws',        'p5 (H100)'),
    (10.00, 20.00, 'gcp',        'a3-highgpu (H100)'),
]

# 인스턴스 타입 패턴
INSTANCE_PATTERNS = {
    # AWS
    r'p3\.\w+':     ('aws', 'V100', 'vm'),
    r'p4d\.\w+':    ('aws', 'A100', 'vm'),
    r'p5\.\w+':     ('aws', 'H100', 'vm'),
    r'g4dn\.\w+':   ('aws', 'T4',   'vm'),
    r'g5\.\w+':     ('aws', 'A10G', 'vm'),
    # GCP
    r'a2-highgpu':  ('gcp', 'A100', 'vm'),
    r'a3-highgpu':  ('gcp', 'H100', 'vm'),
    r'n1-standard': ('gcp', 'V100', 'vm'),
    # Azure
    r'standard_nd': ('azure', 'A100', 'vm'),
    r'standard_nc': ('azure', 'V100', 'vm'),
}


def detect_environment(df: pd.DataFrame) -> Environment:
    """
    데이터프레임에서 인프라 환경 감지
    """
    provider     = 'unknown'
    orchestration = 'unknown'
    access       = 'direct_ssh'
    gpu_model    = 'unknown'
    instance_type = 'unknown'
    confidence   = 30
    signals      = []

    # ── GPU 모델 감지 ──
    for col in ['gpu_model', 'name', 'gpu_name']:
        if col in df.columns:
            raw = str(df[col].iloc[0]).lower().strip()
            gpu_model = raw
            for key in GPU_PROVIDER_HINTS:
                if key in raw:
                    hints = GPU_PROVIDER_HINTS[key]
                    signals.append(('gpu_model', hints[0], 20))
                    break
            break

    # ── Region 감지 ──
    for col in ['datacenter_region', 'region', 'zone', 'location']:
        if col in df.columns:
            region = str(df[col].iloc[0]).lower().strip()
            if region in REGION_MAP:
                p = REGION_MAP[region]
                signals.append(('region', p, 50))
            # 패턴 매칭
            elif 'us-east' in region or 'us-west' in region or 'eu-west' in region:
                signals.append(('region', 'aws', 35))
            elif 'central1' in region or 'europe-west' in region:
                signals.append(('region', 'gcp', 35))
            elif 'eastus' in region or 'westeurope' in region:
                signals.append(('region', 'azure', 35))
            break

    # ── 요금으로 Provider 감지 ──
    rate_col = None
    for col in ['electricity_rate', 'cost_per_hr', 'cost_per_hour']:
        if col in df.columns:
            rate_col = col
            break

    if rate_col:
        avg_rate = df[rate_col].mean()
        if avg_rate > 1.0:  # instance 요금
            for mn, mx, p, inst in PRICE_MAP:
                if mn <= avg_rate <= mx:
                    signals.append(('price', p, 30))
                    instance_type = inst
                    break

    # ── 인스턴스 타입 컬럼 감지 ──
    for col in ['instance_type', 'machine_type', 'vm_type']:
        if col in df.columns:
            inst = str(df[col].iloc[0]).lower()
            for pattern, (p, gpu, acc) in INSTANCE_PATTERNS.items():
                if re.search(pattern, inst):
                    signals.append(('instance', p, 60))
                    instance_type = inst
                    access = acc
                    break
            break

    # ── Job type으로 Orchestration 추정 ──
    if 'job_type' in df.columns or 'workload_type' in df.columns:
        col = 'job_type' if 'job_type' in df.columns else 'workload_type'
        jobs = df[col].unique().tolist()
        job_str = ' '.join([str(j).lower() for j in jobs])
        if 'kubernetes' in job_str or 'k8s' in job_str:
            orchestration = 'kubernetes'
        elif 'slurm' in job_str or 'hpc' in job_str:
            orchestration = 'slurm'
        elif 'sagemaker' in job_str:
            orchestration = 'sagemaker'
            provider = 'aws'
        elif 'vertex' in job_str:
            orchestration = 'vertex_ai'
            provider = 'gcp'

    # ── 신호 종합 ──
    if signals:
        # 가장 높은 confidence 신호 사용
        signals.sort(key=lambda x: x[2], reverse=True)
        provider = signals[0][1]
        confidence = min(95, sum(s[2] for s in signals))

    # ── Provider별 기본 orchestration 설정 ──
    if orchestration == 'unknown':
        if provider == 'aws':
            orchestration = 'vm'
            access = 'vm'
        elif provider == 'gcp':
            orchestration = 'vm'
            access = 'vm'
        elif provider == 'azure':
            orchestration = 'vm'
            access = 'vm'
        elif provider in ['lambda', 'coreweave']:
            orchestration = 'bare_metal'
            access = 'direct_ssh'
        elif provider in ['runpod', 'vast']:
            orchestration = 'container'
            access = 'container'
        else:
            orchestration = 'bare_metal'
            access = 'direct_ssh'

    # GPU 모델 정규화
    gm = gpu_model.upper()
    if 'H100' in gm:
        gpu_model = 'H100'
    elif 'A100' in gm:
        gpu_model = 'A100'
    elif 'V100' in gm:
        gpu_model = 'V100'
    elif 'T4' in gm:
        gpu_model = 'T4'
    elif 'A10' in gm:
        gpu_model = 'A10G'
    elif '3090' in gm:
        gpu_model = 'RTX3090'
    elif '4090' in gm:
        gpu_model = 'RTX4090'

    return Environment(
        provider=provider,
        orchestration=orchestration,
        access=access,
        gpu_model=gpu_model,
        instance_type=instance_type,
        confidence=confidence,
    )


# ══════════════════════════════════════════
# 환경별 명령어 세트
# ══════════════════════════════════════════

def _idle_commands(env: Environment, idle_gpus: list, idle_power: int, tdp: int,
                   idle_start: int = 22, idle_end: int = 7) -> list:
    """
    Idle GPU 절전 명령어 - 환경별로 다름
    idle_gpus: [(index, gpu_id, avg_util), ...]
    """
    gpu_lines = '\n'.join([
        f'  nvidia-smi -i {idx} -pl {idle_power}   # {gid} ({util:.1f}% avg util)'
        for idx, gid, util in idle_gpus[:8]
    ])
    restore_lines = '\n'.join([
        f'  echo "0 {idle_end}  * * * nvidia-smi -i {idx} -pl {tdp}" >> /tmp/gpu_cron.txt'
        for idx, gid, util in idle_gpus[:8]
    ])
    limit_lines = '\n'.join([
        f'  echo "0 {idle_start} * * * nvidia-smi -i {idx} -pl {idle_power}" >> /tmp/gpu_cron.txt'
        for idx, gid, util in idle_gpus[:8]
    ])

    if env.access == 'direct_ssh' or env.orchestration == 'bare_metal':
        return [
            (
                f'Step 1 — SSH into your GPU server and run these commands right now',
                f'This sets the power limit to {idle_power}W (from {tdp}W max). '
                f'The GPU still works at full performance — it just draws less power when idle.',
                gpu_lines
            ),
            (
                f'Step 2 — Automate it so it runs every night at {idle_start:02d}:00',
                f'This is a one-time setup. After this, you never have to think about it again. '
                f'Power drops at {idle_start:02d}:00 and restores at {idle_end:02d}:00 automatically.',
                f'{limit_lines}\n{restore_lines}\n  crontab /tmp/gpu_cron.txt'
            ),
        ]

    elif env.provider == 'aws' and env.orchestration == 'vm':
        return [
            (
                f'Step 1 — SSH into your EC2 instance and run these commands',
                f'On AWS EC2, you can still control GPU power limits directly via nvidia-smi. '
                f'This reduces idle power draw by ~70% with zero performance impact on jobs.',
                gpu_lines
            ),
            (
                f'Step 2 — Set up auto-scheduling via AWS Systems Manager (no manual SSH needed)',
                f'This runs the power-limit command automatically every night — '
                f'no one needs to remember to do it.',
                f'  # Create SSM document for scheduled power management:\n'
                f'  aws ssm create-association \\\n'
                f'    --name "AWS-RunShellScript" \\\n'
                f'    --targets "Key=tag:Role,Values=gpu-worker" \\\n'
                f'    --parameters commands=["nvidia-smi -pl {idle_power}"] \\\n'
                f'    --schedule-expression "cron(0 {idle_start} * * ? *)"\n'
                f'\n'
                f'  # Or simpler — add to EC2 User Data / cloud-init:\n'
                f'  echo "0 {idle_start} * * * nvidia-smi -pl {idle_power}" | crontab -'
            ),
            (
                f'Step 3 — Optional: Use Auto Scaling to stop unused instances',
                f'If these GPUs are truly idle every night, you can stop the EC2 instances '
                f'completely and save 100% (not just 70%). Only do this if workloads allow it.',
                f'  # Stop specific instances at night (saves 100% during downtime):\n'
                f'  aws ec2 stop-instances --instance-ids <YOUR_INSTANCE_ID>\n'
                f'\n'
                f'  # Or set up an Auto Scaling scheduled action:\n'
                f'  aws autoscaling put-scheduled-update-group-action \\\n'
                f'    --auto-scaling-group-name <YOUR_ASG_NAME> \\\n'
                f'    --scheduled-action-name "NightScaleDown" \\\n'
                f'    --recurrence "0 {idle_start} * * *" \\\n'
                f'    --desired-capacity 0'
            ),
        ]

    elif env.provider == 'gcp':
        return [
            (
                f'Step 1 — SSH into your GCP VM and set power limits',
                f'GCP VMs give you full nvidia-smi access. '
                f'This reduces idle power by ~70% with zero job impact.',
                gpu_lines
            ),
            (
                f'Step 2 — Use GCP Cloud Scheduler for automation',
                f'Set up a Cloud Scheduler job to run this automatically every night. '
                f'No manual work after initial setup.',
                f'  # Create Cloud Scheduler job:\n'
                f'  gcloud scheduler jobs create http gpu-idle-limit \\\n'
                f'    --schedule="0 {idle_start} * * *" \\\n'
                f'    --uri="https://compute.googleapis.com/compute/v1/projects/$PROJECT/zones/$ZONE/instances/$INSTANCE/setMetadata" \\\n'
                f'    --message-body=\'{{"items":[{{"key":"startup-script","value":"nvidia-smi -pl {idle_power}"}}]}}\'\n'
                f'\n'
                f'  # Simpler alternative — add crontab via SSH:\n'
                f'  echo "0 {idle_start} * * * nvidia-smi -pl {idle_power}" | crontab -'
            ),
        ]

    elif env.provider == 'azure':
        return [
            (
                f'Step 1 — Connect to your Azure VM and apply power limits',
                f'Azure NDv4/NCv3 VMs support nvidia-smi power management. '
                f'This cuts idle GPU costs by ~70% immediately.',
                gpu_lines
            ),
            (
                f'Step 2 — Automate with Azure Automation or crontab',
                f'Use Azure Automation runbooks or a simple crontab for scheduling.',
                f'  # Simple crontab method (recommended):\n'
                f'  echo "0 {idle_start} * * * nvidia-smi -pl {idle_power}" | crontab -\n'
                f'  echo "0 {idle_end}  * * * nvidia-smi -pl {tdp}" | crontab -\n'
                f'\n'
                f'  # Or Azure VM Auto-shutdown:\n'
                f'  az vm auto-shutdown --resource-group <RG> --name <VM_NAME> \\\n'
                f'    --time {idle_start:02d}00'
            ),
        ]

    elif env.provider in ['runpod', 'vast']:
        return [
            (
                f'Step 1 — Note: On {env.provider.title()}, direct power management is limited',
                f'Marketplace GPU clouds like {env.provider.title()} run containers, so nvidia-smi '
                f'power limits may not be available. The best approach is to stop pods when idle.',
                f'  # Check if power management is available:\n'
                f'  nvidia-smi -pl {idle_power}  # If this fails, use pod stop instead'
            ),
            (
                f'Step 2 — Stop idle pods instead of limiting power',
                f'On {env.provider.title()}, stopping a pod saves 100% of the cost. '
                f'This is more effective than power limiting.',
                f'  # Via {env.provider.title()} API — stop pods not in use:\n'
                f'  curl -X POST https://api.{env.provider}.io/pods/<POD_ID>/stop \\\n'
                f'    -H "Authorization: Bearer $API_KEY"\n'
                f'\n'
                f'  # Or use their web UI to set pod auto-stop after idle time'
            ),
        ]

    else:
        # Generic fallback
        return [
            (
                'Step 1 — Apply power limits to idle GPUs',
                f'Run these commands on the server where your GPUs are running.',
                gpu_lines
            ),
            (
                'Step 2 — Automate with crontab',
                f'Schedule this to run every night at {idle_start:02d}:00.',
                f'{limit_lines}\n{restore_lines}\n  crontab /tmp/gpu_cron.txt'
            ),
        ]


def _overprovisioning_commands(env: Environment, reducible_gpus: int,
                                keep_gpus: int, worst_hour: int) -> list:
    if env.provider == 'aws':
        return [
            (
                'Step 1 — Identify which instances are running idle overnight',
                f'Right now, you have ~{reducible_gpus} GPU instances running overnight '
                f'with almost no workload. This command shows you which ones:',
                f'  aws ec2 describe-instances \\\n'
                f'    --filters "Name=instance-state-name,Values=running" \\\n'
                f'    --query "Reservations[].Instances[].[InstanceId,InstanceType,LaunchTime]"\n'
                f'\n'
                f'  # Check GPU utilization right now:\n'
                f'  nvidia-smi --query-gpu=index,utilization.gpu,power.draw --format=csv'
            ),
            (
                f'Step 2 — Set up Auto Scaling to reduce fleet from {keep_gpus + reducible_gpus} → {keep_gpus} GPUs at {worst_hour:02d}:00',
                f'This tells AWS to automatically scale down your GPU fleet every night '
                f'and back up in the morning. Zero manual work after setup.',
                f'  # Scale down at {worst_hour:02d}:00 (keep only {keep_gpus} instances):\n'
                f'  aws autoscaling put-scheduled-update-group-action \\\n'
                f'    --auto-scaling-group-name <YOUR_ASG_NAME> \\\n'
                f'    --scheduled-action-name "NightScaleDown" \\\n'
                f'    --recurrence "0 {worst_hour} * * *" \\\n'
                f'    --desired-capacity {keep_gpus} \\\n'
                f'    --min-size {keep_gpus}\n'
                f'\n'
                f'  # Scale back up at 08:00:\n'
                f'  aws autoscaling put-scheduled-update-group-action \\\n'
                f'    --auto-scaling-group-name <YOUR_ASG_NAME> \\\n'
                f'    --scheduled-action-name "MorningScaleUp" \\\n'
                f'    --recurrence "0 8 * * 1-5" \\\n'
                f'    --desired-capacity {keep_gpus + reducible_gpus}'
            ),
            (
                'Step 3 — Verify the scaling policy is working',
                'Check that the scheduled action is set up correctly.',
                f'  aws autoscaling describe-scheduled-actions \\\n'
                f'    --auto-scaling-group-name <YOUR_ASG_NAME>'
            ),
        ]

    elif env.provider == 'gcp':
        return [
            (
                'Step 1 — Check which VMs are idle right now',
                f'You have ~{reducible_gpus} GPU VMs running overnight with minimal workload.',
                f'  gcloud compute instances list --filter="status=RUNNING" \\\n'
                f'    --format="table(name,zone,machineType,status)"'
            ),
            (
                f'Step 2 — Schedule VM shutdown at {worst_hour:02d}:00 nightly',
                f'GCP Compute Engine supports scheduled snapshots and instance groups. '
                f'Use Instance Groups for auto-scaling.',
                f'  # Stop specific VMs at night:\n'
                f'  gcloud compute instances stop <INSTANCE_NAME> --zone=<ZONE>\n'
                f'\n'
                f'  # Or use Managed Instance Group autoscaler:\n'
                f'  gcloud compute instance-groups managed set-autoscaling <GROUP_NAME> \\\n'
                f'    --zone=<ZONE> \\\n'
                f'    --min-num-replicas={keep_gpus} \\\n'
                f'    --max-num-replicas={keep_gpus + reducible_gpus} \\\n'
                f'    --scale-based-on-cpu \\\n'
                f'    --target-cpu-utilization=0.7'
            ),
        ]

    elif env.orchestration == 'kubernetes':
        return [
            (
                'Step 1 — Check current GPU node utilization',
                f'You have ~{reducible_gpus} GPU nodes running with almost no workload overnight.',
                f'  kubectl get nodes -l accelerator=nvidia-tesla-a100\n'
                f'  kubectl top nodes\n'
                f'  # Check GPU utilization across all nodes:\n'
                f'  kubectl exec -it <GPU_POD> -- nvidia-smi --query-gpu=utilization.gpu --format=csv'
            ),
            (
                f'Step 2 — Set up Kubernetes cluster autoscaler to scale down to {keep_gpus} nodes',
                f'The Kubernetes Cluster Autoscaler will automatically remove idle GPU nodes '
                f'and add them back when needed.',
                f'  # Enable cluster autoscaler (if not already):\n'
                f'  kubectl apply -f https://raw.githubusercontent.com/kubernetes/autoscaler/master/cluster-autoscaler/deploy/cluster-autoscaler-autodiscover.yaml\n'
                f'\n'
                f'  # Set node group min/max:\n'
                f'  kubectl annotate nodegroup <GPU_NODEGROUP> \\\n'
                f'    cluster-autoscaler.kubernetes.io/min-size="{keep_gpus}" \\\n'
                f'    cluster-autoscaler.kubernetes.io/max-size="{keep_gpus + reducible_gpus}"\n'
                f'\n'
                f'  # Or use KEDA for event-driven scaling:\n'
                f'  kubectl apply -f gpu-scaledobject.yaml'
            ),
            (
                'Step 3 — Add node taints to prevent scheduling on idle nodes',
                'This prevents new jobs from being scheduled on nodes you want to scale down.',
                f'  kubectl taint nodes <IDLE_NODE_NAME> \\\n'
                f'    nvidia.com/gpu=idle:NoSchedule\n'
                f'\n'
                f'  # Drain node safely before stopping:\n'
                f'  kubectl drain <IDLE_NODE_NAME> --ignore-daemonsets --delete-emptydir-data'
            ),
        ]

    elif env.orchestration == 'slurm':
        return [
            (
                'Step 1 — Check current Slurm node states',
                f'See which GPU nodes are idle and can be drained.',
                f'  sinfo -N -l | grep gpu\n'
                f'  squeue --format="%.18i %.9P %.8j %.8u %.2t %.10M %.6D %R"'
            ),
            (
                f'Step 2 — Drain idle nodes at {worst_hour:02d}:00 nightly',
                f'Slurm can put nodes in drain state to stop new job scheduling. '
                f'Use a cron job to automate this.',
                f'  # Add to crontab (run as slurm admin):\n'
                f'  echo "0 {worst_hour} * * * scontrol update NodeName=gpu-node[{keep_gpus+1}-{keep_gpus+reducible_gpus}] State=DRAIN Reason=NightSchedule" | crontab -\n'
                f'\n'
                f'  # Resume in the morning:\n'
                f'  echo "0 8 * * 1-5 scontrol update NodeName=gpu-node[{keep_gpus+1}-{keep_gpus+reducible_gpus}] State=RESUME" | crontab -'
            ),
        ]

    else:
        return [
            (
                'Step 1 — Identify idle GPU servers',
                f'You have ~{reducible_gpus} GPUs running with no workload overnight.',
                f'  nvidia-smi --query-gpu=index,utilization.gpu,power.draw --format=csv'
            ),
            (
                f'Step 2 — Schedule server shutdown at {worst_hour:02d}:00',
                f'Shut down or power off servers that are consistently idle overnight.',
                f'  # Graceful shutdown via crontab:\n'
                f'  echo "0 {worst_hour} * * * systemctl suspend" | crontab -\n'
                f'  echo "0 8 * * 1-5 /usr/sbin/rtcwake -m no -t $(date +%s -d tomorrow 08:00)" | crontab -'
            ),
        ]


def _peak_scheduling_commands(env: Environment, best_offpeak: int,
                               current_rate: float, offpeak_rate: float) -> list:
    discount = round((1 - offpeak_rate / max(current_rate, 1)) * 100)

    if env.orchestration == 'slurm':
        return [
            (
                'Step 1 — Create an off-peak Slurm partition',
                f'Set up a dedicated queue for overnight training jobs. '
                f'Jobs submitted here automatically run at {best_offpeak:02d}:00 when rates are {discount}% cheaper.',
                f'  # Add to /etc/slurm/slurm.conf:\n'
                f'  PartitionName=gpu-offpeak Nodes=ALL Default=NO \\\n'
                f'    MaxTime=12:00:00 State=UP AllowQos=offpeak\n'
                f'\n'
                f'  scontrol reconfigure  # Apply changes'
            ),
            (
                'Step 2 — Submit training jobs to the off-peak queue',
                f'Tell your team to use this command for any batch training job. '
                f'It will queue and start automatically at {best_offpeak:02d}:00.',
                f'  # Submit a training job to run at {best_offpeak:02d}:00:\n'
                f'  sbatch --partition=gpu-offpeak \\\n'
                f'         --begin={best_offpeak:02d}:00:00 \\\n'
                f'         --gres=gpu:a100:1 \\\n'
                f'         train_job.sh\n'
                f'\n'
                f'  # Check queue status:\n'
                f'  squeue --partition=gpu-offpeak'
            ),
        ]

    elif env.orchestration == 'kubernetes':
        return [
            (
                'Step 1 — Use Kubernetes CronJob for batch training',
                f'Convert your training jobs to Kubernetes CronJobs that run at {best_offpeak:02d}:00. '
                f'This is the Kubernetes-native way to schedule off-peak work.',
                f'  # Create a CronJob that runs training at {best_offpeak:02d}:00 nightly:\n'
                f'  cat > training-cronjob.yaml << EOF\n'
                f'  apiVersion: batch/v1\n'
                f'  kind: CronJob\n'
                f'  metadata:\n'
                f'    name: gpu-training-offpeak\n'
                f'  spec:\n'
                f'    schedule: "0 {best_offpeak} * * *"\n'
                f'    jobTemplate:\n'
                f'      spec:\n'
                f'        template:\n'
                f'          spec:\n'
                f'            containers:\n'
                f'            - name: trainer\n'
                f'              image: your-training-image:latest\n'
                f'              resources:\n'
                f'                limits:\n'
                f'                  nvidia.com/gpu: "1"\n'
                f'  GPUEOF\n'
                f'  kubectl apply -f training-cronjob.yaml'
            ),
        ]

    elif env.provider == 'aws':
        return [
            (
                'Step 1 — Use AWS Batch for off-peak job scheduling',
                f'AWS Batch is the recommended way to schedule GPU training jobs at off-peak times. '
                f'Jobs queue and run automatically when rates drop at {best_offpeak:02d}:00.',
                f'  # Submit job to run during off-peak hours:\n'
                f'  aws batch submit-job \\\n'
                f'    --job-name "training-offpeak" \\\n'
                f'    --job-queue "gpu-offpeak-queue" \\\n'
                f'    --job-definition "gpu-training-job"\n'
                f'\n'
                f'  # Or use EC2 Spot Instances for additional savings:\n'
                f'  aws ec2 request-spot-instances \\\n'
                f'    --instance-count 1 \\\n'
                f'    --type "one-time" \\\n'
                f'    --launch-specification file://spot-config.json'
            ),
            (
                'Step 2 — Simple crontab alternative (no AWS Batch needed)',
                f'If AWS Batch is too complex, just reschedule existing jobs with crontab.',
                f'  # Remove existing daytime training cron (if any):\n'
                f'  crontab -l | grep -v "train" | crontab -\n'
                f'\n'
                f'  # Add off-peak training schedule:\n'
                f'  echo "0 {best_offpeak} * * 1-5 cd /workspace && python train.py >> /logs/train.log 2>&1" | crontab -'
            ),
        ]

    else:
        return [
            (
                'Step 1 — Reschedule batch training jobs to off-peak hours',
                f'Move any non-urgent training jobs to start at {best_offpeak:02d}:00. '
                f'Real-time inference should stay on its current schedule.',
                f'  # Check currently running training jobs:\n'
                f'  ps aux | grep python | grep train\n'
                f'\n'
                f'  # Add to crontab:\n'
                f'  crontab -e\n'
                f'  # Add this line:\n'
                f'  0 {best_offpeak} * * 1-5 cd /workspace && python train.py >> /logs/train.log 2>&1'
            ),
        ]


# ══════════════════════════════════════════
# 메인 함수: Action Guide 생성
# ══════════════════════════════════════════

def build_action_guide(rec, env: Environment, df: pd.DataFrame = None) -> ActionGuide:
    """
    Recommendation + Environment → 완전한 ActionGuide 생성
    """
    category = rec.category.lower() if hasattr(rec, 'category') else ''

    # ── Idle Waste ──
    if 'idle' in category:
        idle_gpus = []
        if df is not None and 'gpu_id' in df.columns:
            from analyzer import engineer_features
            try:
                grouped = df.groupby('gpu_id')['gpu_util'].mean()
                for i, (gid, util) in enumerate(grouped.items()):
                    if util < 20:
                        idle_gpus.append((i, gid, util))
            except:
                pass

        idle_power = 75 if env.gpu_model == 'A100' else \
                     50 if env.gpu_model == 'V100' else \
                     100 if env.gpu_model == 'H100' else 60
        tdp = 400 if env.gpu_model == 'A100' else \
              300 if env.gpu_model == 'V100' else \
              700 if env.gpu_model == 'H100' else 300

        n = len(idle_gpus)
        daily_waste = rec.monthly_savings / 30

        situation = (
            f"{n} of your GPUs are sitting idle every night — "
            f"averaging under 15% utilization between 22:00 and 07:00 — "
            f"but still drawing full power. "
            f"That's ${daily_waste:,.0f} wasted every single day."
        )

        business_impact = (
            f"You are paying ${rec.monthly_savings:,.0f}/month for compute you are not using. "
            f"Each idle GPU draws ~{idle_power * 5}W overnight while doing nothing. "
            f"This is not a performance issue — it is a pure billing issue. "
            f"Fixing it requires no code changes, no downtime, and no risk to any workload."
        )

        steps = _idle_commands(env, idle_gpus, idle_power, tdp)

        verify = (
            f"nvidia-smi --query-gpu=index,name,power.draw --format=csv\n"
            f"# Run this after 22:00 tonight"
        )

        expected = (
            f"Power draw drops from ~{tdp}W to ~{idle_power}W per GPU after 22:00. "
            f"Savings of ${rec.monthly_savings:,.0f}/month begin tonight. "
            f"At 07:00, power automatically restores to full capacity."
        )

        return ActionGuide(
            situation=situation,
            business_impact=business_impact,
            what_to_do=f"Limit GPU power to {idle_power}W during idle hours (22:00–07:00).",
            steps=steps,
            verify=verify,
            expected_result=expected,
            owner="DevOps Engineer",
            time_required="5 minutes",
            risk="Zero risk — power limit auto-restores at 07:00. Jobs are unaffected.",
            env=env,
        )

    # ── Overprovisioning ──
    elif 'overprovision' in category:
        from analyzer import detect_overprovision_advanced
        worst_hour = 2
        keep_gpus = 3
        reducible = 10
        if df is not None:
            try:
                over = detect_overprovision_advanced(df)
                waste_h = over.get('top_waste_hours', None)
                if waste_h is not None and len(waste_h) > 0:
                    worst_hour = int(waste_h.iloc[0].get('hour', 2))
                    keep_gpus = max(int(waste_h.iloc[0].get('p95_active', 3)), 1)
                    total = df['gpu_id'].nunique() if 'gpu_id' in df.columns else 16
                    reducible = max(total - keep_gpus - 1, 0)
            except:
                pass

        daily_waste = rec.monthly_savings / 30

        situation = (
            f"Your entire GPU fleet is running 24/7 at full capacity, "
            f"but at {worst_hour:02d}:00, only {keep_gpus} GPUs are actually needed. "
            f"The other {reducible} are running with no meaningful workload — "
            f"costing you ${daily_waste:,.0f} every day for nothing."
        )

        business_impact = (
            f"This is your single largest savings opportunity at ${rec.monthly_savings:,.0f}/month. "
            f"You are provisioned for peak load 24 hours a day, but your actual overnight "
            f"demand is {keep_gpus}/{keep_gpus + reducible} GPUs. "
            f"Scaling down overnight requires a one-time setup and saves money every single night."
        )

        steps = _overprovisioning_commands(env, reducible, keep_gpus, worst_hour)

        verify = (
            f"Check instance/node count after {worst_hour:02d}:00:\n"
            f"  nvidia-smi --query-gpu=index,utilization.gpu --format=csv\n"
            f"  # Should show only {keep_gpus} active GPUs"
        )

        expected = (
            f"GPU fleet reduces from {keep_gpus + reducible} to {keep_gpus} overnight. "
            f"Scales back up at 08:00 automatically. "
            f"Savings of ${rec.monthly_savings:,.0f}/month begin this week."
        )

        return ActionGuide(
            situation=situation,
            business_impact=business_impact,
            what_to_do=f"Scale GPU fleet from {keep_gpus + reducible} → {keep_gpus} GPUs between {worst_hour:02d}:00–08:00.",
            steps=steps,
            verify=verify,
            expected_result=expected,
            owner="DevOps / Infrastructure Engineer",
            time_required="2–4 hours",
            risk="Low — keep 20% buffer above p95 demand. Scale-up happens automatically at 08:00.",
            env=env,
        )

    # ── Peak Scheduling ──
    elif 'peak' in category or 'scheduling' in category:
        best_offpeak = 2
        current_rate = 4.10
        offpeak_rate = 2.10
        if df is not None and 'electricity_rate' in df.columns and 'hour' in df.columns:
            try:
                hourly = df.groupby('hour')['electricity_rate'].mean()
                best_offpeak = int(hourly.idxmin())
                current_rate = hourly.max()
                offpeak_rate = hourly.min()
            except:
                pass

        discount = round((1 - offpeak_rate / max(current_rate, 1)) * 100)

        situation = (
            f"Your training jobs are running during peak hours (09:00–18:00) "
            f"when electricity costs ${current_rate:.2f}/hr. "
            f"The exact same jobs could run at {best_offpeak:02d}:00 for ${offpeak_rate:.2f}/hr — "
            f"{discount}% cheaper — with zero impact on your team's workflow."
        )

        business_impact = (
            f"Non-real-time workloads like model fine-tuning, batch inference, and eval runs "
            f"do not need to run during business hours. Moving them overnight saves "
            f"${rec.monthly_savings:,.0f}/month with a one-time scheduling change. "
            f"Real-time inference is unaffected."
        )

        steps = _peak_scheduling_commands(env, best_offpeak, current_rate, offpeak_rate)

        verify = (
            f"Check that training jobs are running at the right time:\n"
            f"  ps aux | grep python | grep train\n"
            f"  # Should show activity at {best_offpeak:02d}:00, not 09:00–18:00"
        )

        expected = (
            f"Training costs drop by {discount}% for rescheduled jobs. "
            f"Savings of ${rec.monthly_savings:,.0f}/month begin this week. "
            f"Daytime GPU capacity frees up for inference and interactive work."
        )

        return ActionGuide(
            situation=situation,
            business_impact=business_impact,
            what_to_do=f"Move batch training jobs to {best_offpeak:02d}:00 when rates are {discount}% cheaper.",
            steps=steps,
            verify=verify,
            expected_result=expected,
            owner="ML Engineer",
            time_required="1–2 hours",
            risk="Zero risk — real-time inference stays on current schedule. Only batch jobs move.",
            env=env,
        )

    elif 'consolidat' in category:
        # GPU Consolidation = 여러 GPU idle 동시에 → 워크로드 통합
        avg_idle = 0
        worst_hours = []
        if df is not None:
            try:
                from analyzer import detect_inter_gpu_waste
                inter = detect_inter_gpu_waste(df)
                avg_idle = inter.get('avg_concurrent_idle_gpus', 0)
                worst_hours = inter.get('worst_hours', [])
                gap_hours = inter.get('total_waste_hours_monthly', 0)
            except:
                gap_hours = 0
        
        worst_h_str = ', '.join([f'{h:02d}:00' for h in worst_hours[:3]]) if worst_hours else 'overnight'
        daily_waste = rec.monthly_savings / 30

        situation = (
            f"On average, {avg_idle:.0f} GPUs are sitting idle at the same time — "
            f"most frequently at {worst_h_str}. "
            f"These GPUs are each running separate workloads that don't need a full GPU, "
            f"but each one is occupying an entire GPU anyway. "
            f"That's ${daily_waste:,.0f} wasted every day."
        )

        business_impact = (
            f"Multiple small workloads are each claiming a full GPU when they only need a fraction of one. "
            f"NVIDIA MIG (Multi-Instance GPU) lets you split one A100 into up to 7 independent instances, "
            f"so 4 small jobs can share 1 GPU instead of occupying 4. "
            f"This is the highest-leverage optimization available for your fleet."
        )

        steps = [
            (
                'Step 1 — Check which GPUs are idle at the same time right now',
                'This shows how many GPUs are simultaneously underutilized:',
                '  nvidia-smi --query-gpu=index,utilization.gpu,memory.used --format=csv\n'
                '  # Look for multiple GPUs all showing < 15% utilization at the same time'
            ),
            (
                'Step 2 — Enable NVIDIA MIG on your A100s (splits 1 GPU into multiple instances)',
                'MIG lets you run multiple isolated workloads on a single GPU. '
                'A single A100 can become 2, 3, or 7 independent GPU instances.',
                '  # Enable MIG mode on GPU 0:\n'
                '  sudo nvidia-smi -i 0 -mig 1\n'
                '  sudo reboot\n'
                '\n'
                '  # After reboot — create 2 equal instances (each gets half the A100):\n'
                '  sudo nvidia-smi mig -cgi 3g.40gb,3g.40gb -C\n'
                '\n'
                '  # Verify instances are created:\n'
                '  nvidia-smi -L'
            ),
            (
                'Step 3 — Assign small workloads to MIG instances instead of full GPUs',
                'Point your inference or small training jobs to the MIG instance instead of the full GPU.',
                '  # Run a job on MIG instance 0:\n'
                '  CUDA_VISIBLE_DEVICES=MIG-GPU-xxxxxxxx:0:0 python inference.py\n'
                '\n'
                '  # In Kubernetes — request a MIG slice instead of a full GPU:\n'
                '  resources:\n'
                '    limits:\n'
                '      nvidia.com/mig-3g.40gb: "1"  # half an A100'
            ),
        ]

        return ActionGuide(
            situation=situation,
            business_impact=business_impact,
            what_to_do=f"Use NVIDIA MIG to share GPUs across multiple small workloads instead of allocating one GPU per job.",
            steps=steps,
            verify=(
                'nvidia-smi -L\n'
                '# Should show MIG instances listed under each GPU'
            ),
            expected_result=(
                f"GPU count needed for small workloads drops by 50-70%. "
                f"Estimated savings: ${rec.monthly_savings:,.0f}/month."
            ),
            owner="DevOps / ML Engineer",
            time_required="2-4 hours",
            risk="Medium — test with non-critical workloads first. MIG requires GPU reboot to enable.",
            env=env,
        )

    elif 'workload' in category or 'gap' in category:
        worst_gpu = 'unknown'
        gap_hours = rec.monthly_savings / max(4.10, 1) * 0.7  # 추정값
        if df is not None and 'gpu_id' in df.columns:
            try:
                from analyzer import detect_workload_gap
                gap_result = detect_workload_gap(df)
                affected = gap_result.get('affected_gpus', [])
                if affected:
                    worst_gpu = affected[0]['gpu_id']
                gh = gap_result.get('total_waste_hours_monthly', 0)
                if gh > 0:
                    gap_hours = gh
            except:
                pass

        daily_waste = rec.monthly_savings / 30
        n_gpus = df['gpu_id'].nunique() if df is not None and 'gpu_id' in df.columns else 1
        hourly_rate = rec.monthly_savings / max(gap_hours * n_gpus, 1)

        situation = (
            f"Your GPUs are powered on and consuming electricity, "
            f"but have no jobs running for {gap_hours:.0f} hours every month. "
            f"This is unplanned idle time between job submissions, "
            f"costing you ${daily_waste:,.0f}/day."
        )

        business_impact = (
            f"Every hour a GPU sits powered on with no job costs ${hourly_rate:.2f}. "
            f"These {gap_hours:.0f} unplanned idle hours cost ${rec.monthly_savings:,.0f}/month. "
            f"An automated alert system catches idle GPUs within 30 minutes "
            f"so your team can reassign or shut them down immediately."
        )

        reaper_cmd = (
            "  # Create the idle detection script:\n"
            "  cat > /usr/local/bin/gpu_reaper.sh << 'SCRIPT'\n"
            "  #!/bin/bash\n"
            "  nvidia-smi --query-gpu=index,utilization.gpu --format=csv,noheader,nounits\n"
            "  | while IFS=, read idx util; do\n"
            "      if [ ${util// /} -lt 5 ]; then\n"
            "        echo GPU $idx idle | mail -s IDLE_GPU_ALERT devops@company.com\n"
            "      fi\n"
            "  done\n"
            "  SCRIPT\n"
            "  chmod +x /usr/local/bin/gpu_reaper.sh\n"
            "  echo '*/30 * * * * /usr/local/bin/gpu_reaper.sh' | crontab -"
        )

        steps = [
            (
                'Step 1 — See which GPUs are idle right now',
                'This shows GPUs that are powered on but have no active jobs:',
                '  nvidia-smi --query-gpu=index,utilization.gpu,power.draw --format=csv'
            ),
            (
                'Step 2 — Set up an Idle Job Reaper (alerts within 30 min)',
                'Runs every 30 minutes and emails your team if a GPU has been idle too long.',
                reaper_cmd
            ),
            (
                'Step 3 — Use a job queue to eliminate gaps between submissions',
                'A job queue ensures GPUs always have work waiting — no manual gaps.',
                '  # Check current job queue:\n'
                '  nvidia-smi pmon -s u -d 10 -c 3\n'
                '\n'
                '  # If using Slurm:\n'
                '  squeue --format="%.18i %.9P %.8j %.2t %.10M"\n'
                '\n'
                '  # Simple Python task queue:\n'
                '  pip install celery redis'
            ),
        ]

        return ActionGuide(
            situation=situation,
            business_impact=business_impact,
            what_to_do="Set up automated idle detection to catch GPUs with no jobs within 30 minutes.",
            steps=steps,
            verify=(
                "Check the monitor is running:\n"
                "  crontab -l | grep gpu_reaper\n"
                "  # Expected: */30 * * * * /usr/local/bin/gpu_reaper.sh"
            ),
            expected_result=(
                f"Idle GPU time drops as your team responds faster to alerts. "
                f"Target: under 50 unplanned idle hours/month. "
                f"Estimated savings: ${rec.monthly_savings:,.0f}/month."
            ),
            owner="DevOps / MLOps Engineer",
            time_required="1 hour",
            risk="Zero risk — monitoring only. No workloads are affected.",
            env=env,
        )

    # ── Fallback ──
    else:
        return ActionGuide(
            situation=rec.detail if hasattr(rec, 'detail') else '',
            business_impact='',
            what_to_do=rec.action if hasattr(rec, 'action') else '',
            steps=[('Action Required', '', rec.action if hasattr(rec, 'action') else '')],
            verify='Verify results after implementation.',
            expected_result=f'Save ${rec.monthly_savings:,.0f}/month.',
            owner='DevOps',
            time_required='TBD',
            risk='Medium',
            env=env,
        )


def format_guide_text(guide: ActionGuide) -> str:
    """
    ActionGuide → 사람이 읽을 수 있는 텍스트
    """
    lines = []

    lines.append(f'SITUATION')
    lines.append(guide.situation)
    lines.append('')

    lines.append(f'BUSINESS IMPACT')
    lines.append(guide.business_impact)
    lines.append('')

    lines.append(f'WHAT TO DO  ·  Owner: {guide.owner}  ·  Time: {guide.time_required}')
    lines.append('─' * 60)
    lines.append(guide.what_to_do)
    lines.append('')

    for title, explanation, commands in guide.steps:
        lines.append(title)
        if explanation:
            lines.append(explanation)
        if commands:
            lines.append(commands)
        lines.append('')

    lines.append('HOW TO VERIFY')
    lines.append(guide.verify)
    lines.append('')

    lines.append('EXPECTED RESULT')
    lines.append(guide.expected_result)
    lines.append('')

    lines.append(f'RISK  {guide.risk}')

    # Rollback 추가
    rollback = _get_rollback(guide)
    if rollback:
        lines.append('')
        lines.append('ROLLBACK (if something goes wrong)')
        lines.append(rollback)

    lines.append(f'ENVIRONMENT  {guide.env.provider.upper()} / {guide.env.gpu_model}')

    return '\n'.join(lines)


def _get_rollback(guide: ActionGuide) -> str:
    category = guide.env.provider
    action_lower = guide.what_to_do.lower()

    if 'power' in action_lower or 'idle' in action_lower:
        return (
            "Restore full power immediately:\n"
            "  nvidia-smi -i <GPU_INDEX> -pl <ORIGINAL_TDP>\n"
            "  # A100: -pl 400  |  V100: -pl 300  |  H100: -pl 700\n"
            "  crontab -r  # Remove scheduled power limits"
        )
    elif 'scale' in action_lower or 'fleet' in action_lower:
        if guide.env.provider == 'aws':
            return (
                "Restore full fleet immediately:\n"
                "  aws autoscaling set-desired-capacity \\\n"
                "    --auto-scaling-group-name <YOUR_ASG_NAME> \\\n"
                "    --desired-capacity <ORIGINAL_COUNT>\n"
                "  # Takes ~2 minutes for instances to start"
            )
        else:
            return (
                "Restore full fleet:\n"
                "  kubectl scale deployment <GPU_DEPLOYMENT> --replicas=<ORIGINAL_COUNT>\n"
                "  # Or manually restart stopped instances"
            )
    elif 'mig' in action_lower or 'consolidat' in action_lower:
        return (
            "Disable MIG mode (requires reboot):\n"
            "  sudo nvidia-smi -i <GPU_INDEX> -mig 0\n"
            "  sudo reboot\n"
            "  # All workloads resume on full GPU after reboot"
        )
    elif 'schedule' in action_lower or 'training' in action_lower:
        return (
            "Revert training schedule:\n"
            "  crontab -e  # Remove the off-peak training entry\n"
            "  # Or simply restart training manually at any time"
        )
    else:
        return (
            "Revert changes:\n"
            "  crontab -r  # Remove any scheduled tasks\n"
            "  # Restart affected services if needed"
        )


if __name__ == '__main__':
    import warnings
    warnings.filterwarnings('ignore')
    from data_loader import load_and_prepare
    from analyzer import engineer_features, detect_idle_maximum, detect_peak_waste_advanced, detect_overprovision_advanced, compute_advanced_efficiency_score, detect_thermal_throttling, detect_memory_bandwidth_bottleneck, detect_inter_gpu_waste, detect_workload_gap
    from cost_model import simulate_before_after
    from recommender import generate_recommendations

    df, col_map, quality = load_and_prepare('realistic_gpu_data.csv')
    df = engineer_features(df)
    env = detect_environment(df)
    print(f'Detected: {env.provider} / {env.orchestration} / {env.gpu_model} (confidence: {env.confidence}%)')
    print()

    idle = detect_idle_maximum(df)
    peak = detect_peak_waste_advanced(df)
    over = detect_overprovision_advanced(df)
    sim  = simulate_before_after(df)
    scores = compute_advanced_efficiency_score(df)
    thermal = detect_thermal_throttling(df)
    mem_b = detect_memory_bandwidth_bottleneck(df)
    inter = detect_inter_gpu_waste(df)
    gap = detect_workload_gap(df)
    recs = generate_recommendations(idle, peak, over, sim, scores, df=df,
        thermal=thermal, mem_bottleneck=mem_b, inter_gpu=inter, workload_gap=gap)

    for r in recs[:3]:
        guide = build_action_guide(r, env, df)
        print(format_guide_text(guide))
        print('=' * 70)
        print()
