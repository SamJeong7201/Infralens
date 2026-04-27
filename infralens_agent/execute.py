"""
execute.py
──────────
역할: 승인된 명령만 실행
      기본 OFF — config.yaml에서 enabled: true 해야 작동
      항상 dry_run 먼저 확인
"""
import subprocess
import sqlite3
from datetime import datetime
from pathlib import Path
import yaml

CONFIG_PATH = Path(__file__).parent / 'config.yaml'

def load_config():
    with open(CONFIG_PATH) as f:
        return yaml.safe_load(f)


def log_action(conn, action_type: str, command: str, status: str, result: str):
    conn.execute('''
        INSERT INTO actions_log (timestamp, action_type, command, status, result)
        VALUES (?, ?, ?, ?, ?)
    ''', (datetime.now().isoformat(), action_type, command, status, result))
    conn.commit()


def execute_recommendation(rec: dict, dry_run: bool = True) -> dict:
    """
    권장사항 실행
    dry_run=True  → 명령어만 보여주고 실행 안 함
    dry_run=False → 실제 실행 (config에서 enabled=true 필요)
    """
    config = load_config()

    # 보안 체크
    if not config['execution']['enabled']:
        return {
            'status': 'disabled',
            'message': 'Execution is disabled. Set execution.enabled: true in config.yaml'
        }

    if config['execution']['dry_run'] or dry_run:
        print(f'[DRY RUN] Would execute:')
        print(rec['command'])
        return {'status': 'dry_run', 'command': rec['command']}

    # 실제 실행
    results = []
    for line in rec['command'].strip().split('\n'):
        cmd = line.strip().lstrip('#').strip()
        if not cmd or cmd.startswith('#'):
            continue

        print(f'Executing: {cmd}')
        result = subprocess.run(
            cmd.split(),
            capture_output=True,
            text=True,
            timeout=30
        )

        results.append({
            'command':   cmd,
            'returncode': result.returncode,
            'stdout':    result.stdout.strip(),
            'stderr':    result.stderr.strip(),
        })

        if result.returncode != 0:
            print(f'  ❌ Failed: {result.stderr}')
        else:
            print(f'  ✅ Success')

    return {'status': 'executed', 'results': results}


if __name__ == '__main__':
    # 테스트
    test_rec = {
        'command': 'nvidia-smi --query-gpu=index,power.draw --format=csv',
        'rollback': ''
    }
    result = execute_recommendation(test_rec, dry_run=True)
    print(result)
