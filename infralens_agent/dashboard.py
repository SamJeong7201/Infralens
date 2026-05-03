"""
dashboard.py
────────────
역할: 승인 플로우 웹 대시보드
"""
import streamlit as st
import sqlite3
import pandas as pd
import subprocess
import yaml
from datetime import datetime, timedelta
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent))
from analyze import run_all

CONFIG_PATH = Path(__file__).parent / 'config.yaml'
DB_PATH     = Path(__file__).parent / 'data/metrics.db'

def load_config():
    with open(CONFIG_PATH) as f:
        return yaml.safe_load(f)

def load_data(hours=24) -> pd.DataFrame:
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

def execute_command(command: str, dry_run: bool) -> list:
    results = []
    for line in command.strip().split('\n'):
        cmd = line.strip()
        if not cmd or cmd.startswith('#'):
            continue
        if dry_run:
            results.append({'cmd': cmd, 'status': 'DRY RUN', 'output': '(not executed)'})
            continue
        try:
            r = subprocess.run(cmd.split(), capture_output=True, text=True, timeout=30)
            results.append({
                'cmd'   : cmd,
                'status': 'OK' if r.returncode == 0 else 'FAIL',
                'output': r.stdout.strip() or r.stderr.strip()
            })
        except Exception as e:
            results.append({'cmd': cmd, 'status': 'ERROR', 'output': str(e)})
    return results

# ── UI 설정 ──
st.set_page_config(page_title='InfraLens', page_icon='⚡', layout='wide')
st.markdown('<meta http-equiv="refresh" content="30">', unsafe_allow_html=True)
st.title('⚡ InfraLens — GPU 인프라 모니터')

config  = load_config()
dry_run = config['execution']['dry_run']

if dry_run:
    st.warning('🔒 DRY RUN 모드 — config.yaml에서 dry_run: false 로 변경하면 실제 실행됩니다')

# ── 데이터 로드 + 분석 ──
df = load_data(hours=24)
if df.empty:
    st.error('데이터 없음 — python run.py 먼저 실행하세요')
    st.stop()

result  = run_all(df)
summary = result['summary']
alerts  = result['alerts']
recs    = result['recommendations']

# ── 요약 카드 ──
col1, col2, col3, col4, col5, col6 = st.columns(6)
col1.metric('GPU 수',      summary.get('n_gpus', 0))
col2.metric('평균 사용률', f"{summary.get('overall_util', 0)}%")
col3.metric('전체 전력',   f"{summary.get('total_power_w', 0)}W")
col4.metric('🔴 위험',     summary.get('n_critical', 0))
col5.metric('🟠 높음',     summary.get('n_high', 0))
col6.metric('전체 알림',   f"{summary.get('n_alerts', 0)}개")

st.caption(f'마지막 업데이트: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")} — 30초 자동 새로고침')
st.divider()

# ── 권장사항 (승인 플로우) ──
st.subheader(f'💡 권장사항 ({len(recs)}개)')

if not recs:
    st.success('✅ 현재 권장사항 없음')
else:
    for i, rec in enumerate(recs):
        sev  = rec['severity']
        icon = {'critical': '🔴', 'high': '🟠', 'medium': '🟡', 'low': '🟢'}.get(sev, '⚪')
        auto = '✅ 자동 안전' if rec.get('auto_safe') else '⚠️ 수동 확인 필요'
        expand = sev in ['critical', 'high']

        with st.expander(f"{icon} [{sev.upper()}] {rec['type']} — {auto}", expanded=expand):
            st.write(rec['message'])

            col_cmd, col_roll = st.columns(2)
            with col_cmd:
                st.caption('실행 명령어')
                st.code(rec.get('command', ''), language='bash')
            with col_roll:
                st.caption('롤백 명령어')
                st.code(rec.get('rollback', ''), language='bash')

            col_a, col_r, _ = st.columns([2, 2, 6])
            if col_a.button('✅ 승인 실행', key=f'a_{i}', type='primary'):
                with st.spinner('실행 중...'):
                    res    = execute_command(rec['command'], dry_run=dry_run)
                    status = 'dry_run' if dry_run else 'executed'
                    log_action(rec['type'], rec['command'], status,
                               str([r['output'] for r in res]))
                for r in res:
                    icon2 = '✅' if r['status'] in ['OK', 'DRY RUN'] else '❌'
                    st.write(f"{icon2} `{r['cmd']}` → {r['output']}")

            if col_r.button('❌ 거절', key=f'r_{i}'):
                log_action(rec['type'], rec['command'], 'rejected', 'user rejected')
                st.info('거절됨 — 로그에 기록됐습니다')

st.divider()

# ── 전체 알림 (접혀있음) ──
with st.expander(f'⚠️ 전체 알림 ({len(alerts)}개)', expanded=False):
    for a in alerts:
        icon = {'critical': '🔴', 'high': '🟠', 'medium': '🟡', 'low': '🟢'}.get(a['severity'], '⚪')
        st.write(f"{icon} **{a['type']}** — {a['message']}")

# ── 실행 로그 (접혀있음) ──
with st.expander('📋 실행 로그', expanded=False):
    try:
        conn   = sqlite3.connect(str(DB_PATH))
        log_df = pd.read_sql_query(
            "SELECT timestamp, action_type, status, result FROM actions_log ORDER BY timestamp DESC LIMIT 20",
            conn
        )
        conn.close()
        if log_df.empty:
            st.write('아직 실행 기록 없음')
        else:
            st.dataframe(log_df, use_container_width=True)
    except:
        st.write('로그 없음')
