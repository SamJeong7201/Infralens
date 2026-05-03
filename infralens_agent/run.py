"""
run.py - InfraLens Agent 메인 실행
python run.py              → 한 번 실행
python run.py --loop       → 자동 반복
python run.py --auto       → auto_safe 자동 실행
python run.py --loop --auto → 전체 자동화
"""
import sys
import time
import subprocess
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
import yaml

sys.path.insert(0, str(Path(__file__).parent))

from collect import run_collection
from analyze import run_all
from tracker import save_snapshot, get_changes, get_recurring, print_changes
from env_detect import detect_all, print_env

CONFIG_PATH = Path(__file__).parent / 'config.yaml'
DB_PATH     = Path(__file__).parent / 'data/metrics.db'


def load_config():
    with open(CONFIG_PATH) as f:
        return yaml.safe_load(f)


def load_recent_data(hours=24) -> pd.DataFrame:
    if not DB_PATH.exists():
        return pd.DataFrame()
    conn   = sqlite3.connect(str(DB_PATH))
    cutoff = (datetime.now() - timedelta(hours=hours)).isoformat()
    df     = pd.read_sql_query(
        f"SELECT * FROM gpu_metrics WHERE timestamp > '{cutoff}' ORDER BY timestamp",
        conn
    )
    conn.close()
    return df


def log_action(action_type, command, status, result_text):
    conn = sqlite3.connect(str(DB_PATH))
    conn.execute('''
        INSERT INTO actions_log (timestamp, action_type, command, status, result)
        VALUES (?, ?, ?, ?, ?)
    ''', (datetime.now().isoformat(), action_type, command, status, result_text))
    conn.commit()
    conn.close()


def execute_auto(rec: dict, dry_run: bool) -> list:
    results = []
    for line in rec['command'].strip().split('\n'):
        cmd = line.strip()
        if not cmd or cmd.startswith('#'):
            continue
        if dry_run:
            results.append({'cmd': cmd, 'status': 'DRY RUN', 'output': ''})
            continue
        try:
            r = subprocess.run(
                cmd.split(), capture_output=True, text=True, timeout=30
            )
            results.append({
                'cmd'   : cmd,
                'status': 'OK' if r.returncode == 0 else 'FAIL',
                'output': r.stdout.strip() or r.stderr.strip()
            })
        except Exception as e:
            results.append({'cmd': cmd, 'status': 'ERROR', 'output': str(e)})
    return results


def run_once(auto_execute=False):
    config  = load_config()
    dry_run = config['execution']['dry_run']

    print('=' * 55)
    print(f'InfraLens Agent — {datetime.now().strftime("%Y-%m-%d %H:%M")}')
    print('=' * 55)

    # 0. 환경 감지 (첫 실행시)
    env = detect_all()
    print_env(env)
    print()

    # 1. 수집
    run_collection()
    print()

    # 2. 분석
    df = load_recent_data(hours=24)
    if df.empty:
        print('No data yet')
        return None

    result  = run_all(df)
    summary = result['summary']
    alerts  = result['alerts']
    recs    = result['recommendations']

    # 3. 변화 추적
    save_snapshot(result)
    changes   = get_changes(result)
    recurring = get_recurring(min_occurrences=3)

    # 4. 요약 출력
    print(f"GPUs:         {summary.get('n_gpus', 0)}")
    print(f"Overall util: {summary.get('overall_util', 0)}%")
    print(f"Total power:  {summary.get('total_power_w', 0)}W")
    print(f"Alerts:       {summary.get('n_alerts', 0)} "
          f"({summary.get('n_critical',0)} critical, "
          f"{summary.get('n_high',0)} high, "
          f"{summary.get('n_medium',0)} medium)")
    print()

    # 5. 변화 출력
    print_changes(changes, recurring)
    print()

    # 6. 알림 출력
    if alerts:
        print('⚠️  ALERTS:')
        for a in alerts:
            print(f"  [{a['severity'].upper()}] {a['message']}")
    else:
        print('✅ No alerts')
    print()

    # 7. 자동 실행
    if auto_execute:
        auto_recs = [r for r in recs if r.get('auto_safe')]
        if auto_recs:
            print(f'🤖 AUTO EXECUTE — {len(auto_recs)} safe actions')
            if dry_run:
                print('   (DRY RUN — set dry_run: false to actually execute)')
            for rec in auto_recs:
                print(f"   → {rec['type']}: {rec['message'][:55]}")
                res    = execute_auto(rec, dry_run=dry_run)
                status = 'dry_run' if dry_run else 'auto_executed'
                log_action(rec['type'], rec['command'], status,
                           str([r.get('output','') for r in res]))
                for r in res:
                    icon = '✅' if r['status'] in ['OK','DRY RUN'] else '❌'
                    print(f"     {icon} {r['cmd']}")
        else:
            print('🤖 AUTO EXECUTE — no safe actions')
        print()

    # 8. 수동 확인 필요
    manual_recs = [r for r in recs if not r.get('auto_safe')]
    if manual_recs:
        print(f'⚠️  MANUAL REVIEW ({len(manual_recs)}):')
        for r in manual_recs:
            print(f"  [{r['severity'].upper()}] {r['type']} — {r['message'][:55]}")
    print()

    return result


def run_loop(interval_minutes=5, auto_execute=False):
    print(f'InfraLens Agent — loop every {interval_minutes}min')
    print(f'Auto execute: {"ON" if auto_execute else "OFF"}')
    print('Press Ctrl+C to stop')
    print()
    while True:
        try:
            run_once(auto_execute=auto_execute)
            print(f'Next run in {interval_minutes} minutes...\n')
            time.sleep(interval_minutes * 60)
        except KeyboardInterrupt:
            print('\nStopped.')
            break


if __name__ == '__main__':
    loop = '--loop' in sys.argv
    auto = '--auto' in sys.argv
    config = load_config()

    if loop:
        run_loop(config['collection']['interval_minutes'], auto_execute=auto)
    else:
        run_once(auto_execute=auto)
