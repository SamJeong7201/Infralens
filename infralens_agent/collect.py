"""
collect.py
──────────
역할: GPU 서버에서 데이터 수집
      nvidia-smi / DCGM / Slurm
      외부로 데이터 전송 없음 - 로컬 SQLite에만 저장
"""
import subprocess
import sqlite3
import json
import os
import sys
from datetime import datetime
from pathlib import Path

# 설정 파일 로드
import yaml
CONFIG_PATH = Path(__file__).parent / 'config.yaml'

def load_config():
    with open(CONFIG_PATH) as f:
        return yaml.safe_load(f)

# ── SQLite 초기화 ──
def init_db(db_path: str):
    conn = sqlite3.connect(db_path)
    conn.execute('''
        CREATE TABLE IF NOT EXISTS gpu_metrics (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp   TEXT NOT NULL,
            gpu_index   INTEGER,
            gpu_name    TEXT,
            gpu_util    REAL,
            mem_util    REAL,
            mem_used_mb REAL,
            mem_total_mb REAL,
            power_draw  REAL,
            power_limit REAL,
            temperature REAL,
            fan_speed   REAL
        )
    ''')
    conn.execute('''
        CREATE TABLE IF NOT EXISTS slurm_jobs (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp    TEXT NOT NULL,
            job_id       TEXT,
            user         TEXT,
            job_name     TEXT,
            partition    TEXT,
            state        TEXT,
            gpu_count    INTEGER,
            wait_minutes REAL,
            run_minutes  REAL
        )
    ''')
    conn.execute('''
        CREATE TABLE IF NOT EXISTS actions_log (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp   TEXT NOT NULL,
            action_type TEXT,
            command     TEXT,
            status      TEXT,
            result      TEXT
        )
    ''')
    conn.commit()
    return conn


# ── nvidia-smi 수집 ──
def collect_nvidia_smi(conn):
    """
    nvidia-smi로 GPU 상태 수집
    외부 전송 없음 - 로컬 DB에만 저장
    """
    try:
        import shutil, sys as _sys
        fake_path = Path(__file__).parent / 'fake_nvidia_smi.py'
        if shutil.which('nvidia-smi'):
            nvidia_cmd = ['nvidia-smi']
        elif fake_path.exists():
            nvidia_cmd = [_sys.executable, str(fake_path)]
            print('  [TEST MODE] Using fake nvidia-smi')
        else:
            print('nvidia-smi not found')
            return []

        result = subprocess.run(
            nvidia_cmd + [
                '--query-gpu=index,name,utilization.gpu,utilization.memory,'
                'memory.used,memory.total,power.draw,power.limit,'
                'temperature.gpu,fan.speed',
                '--format=csv,noheader,nounits'
            ], capture_output=True, text=True, timeout=30)

        if result.returncode != 0:
            print(f'nvidia-smi error: {result.stderr}')
            return []

        timestamp = datetime.now().isoformat()
        rows = []

        for line in result.stdout.strip().split('\n'):
            if not line.strip():
                continue
            parts = [p.strip() for p in line.split(',')]
            if len(parts) < 10:
                continue

            def safe_float(val, default=0.0):
                try:
                    return float(val.replace('%','').replace('W','').strip())
                except:
                    return default

            row = {
                'timestamp':    timestamp,
                'gpu_index':    int(parts[0]),
                'gpu_name':     parts[1],
                'gpu_util':     safe_float(parts[2]),
                'mem_util':     safe_float(parts[3]),
                'mem_used_mb':  safe_float(parts[4]),
                'mem_total_mb': safe_float(parts[5]),
                'power_draw':   safe_float(parts[6]),
                'power_limit':  safe_float(parts[7]),
                'temperature':  safe_float(parts[8]),
                'fan_speed':    safe_float(parts[9]),
            }
            rows.append(row)

        conn.executemany('''
            INSERT INTO gpu_metrics
            (timestamp, gpu_index, gpu_name, gpu_util, mem_util,
             mem_used_mb, mem_total_mb, power_draw, power_limit,
             temperature, fan_speed)
            VALUES
            (:timestamp, :gpu_index, :gpu_name, :gpu_util, :mem_util,
             :mem_used_mb, :mem_total_mb, :power_draw, :power_limit,
             :temperature, :fan_speed)
        ''', rows)
        conn.commit()

        print(f'[{timestamp}] Collected {len(rows)} GPUs')
        return rows

    except FileNotFoundError:
        print('nvidia-smi not found - is this a GPU server?')
        return []
    except Exception as e:
        print(f'Collection error: {e}')
        return []


# ── Slurm 수집 (선택) ──
def collect_slurm(conn):
    """
    Slurm job 정보 수집
    squeue + sacct 사용
    """
    try:
        # 현재 queue
        result = subprocess.run([
            'squeue',
            '--format=%i,%u,%j,%P,%T,%r,%V,%S',
            '--noheader'
        ], capture_output=True, text=True, timeout=30)

        if result.returncode != 0:
            return []

        timestamp = datetime.now().isoformat()
        rows = []

        for line in result.stdout.strip().split('\n'):
            if not line.strip():
                continue
            parts = line.split(',')
            if len(parts) < 6:
                continue

            rows.append({
                'timestamp':  timestamp,
                'job_id':     parts[0].strip(),
                'user':       parts[1].strip(),
                'job_name':   parts[2].strip(),
                'partition':  parts[3].strip(),
                'state':      parts[4].strip(),
                'gpu_count':  0,
                'wait_minutes': 0,
                'run_minutes':  0,
            })

        conn.executemany('''
            INSERT INTO slurm_jobs
            (timestamp, job_id, user, job_name, partition, state,
             gpu_count, wait_minutes, run_minutes)
            VALUES
            (:timestamp, :job_id, :user, :job_name, :partition, :state,
             :gpu_count, :wait_minutes, :run_minutes)
        ''', rows)
        conn.commit()

        print(f'[{timestamp}] Collected {len(rows)} Slurm jobs')
        return rows

    except FileNotFoundError:
        print('squeue not found - Slurm not available')
        return []
    except Exception as e:
        print(f'Slurm collection error: {e}')
        return []


# ── 오래된 데이터 정리 ──
def cleanup_old_data(conn, retention_days: int):
    cutoff = datetime.now().isoformat()[:10]
    conn.execute(f'''
        DELETE FROM gpu_metrics
        WHERE timestamp < date('{cutoff}', '-{retention_days} days')
    ''')
    conn.commit()


# ── 메인 ──
def run_collection():
    config = load_config()
    db_path = Path(__file__).parent / config['storage']['db_path']
    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = init_db(str(db_path))

    print('InfraLens Agent — Data Collection')
    print(f'DB: {db_path}')
    print(f'Mode: {config["lab"]["mode"]}')
    print()

    # GPU 수집
    if config['collection']['nvidia_smi']:
        rows = collect_nvidia_smi(conn)
        if not rows:
            print('No GPU data collected')

    # Slurm 수집
    if config['collection']['slurm']:
        collect_slurm(conn)

    # 오래된 데이터 정리
    cleanup_old_data(conn, config['storage']['retention_days'])

    conn.close()
    print('Collection complete')


if __name__ == '__main__':
    run_collection()
