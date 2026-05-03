"""
tracker.py
──────────
역할: 분석 결과 변화 추적
      - 매 실행마다 스냅샷 저장
      - 이전 대비 변화 감지
      - 반복 문제 추적 (N일째 같은 문제)
      - before/after 비교
"""
import sqlite3
import json
from datetime import datetime, timedelta
from pathlib import Path

DB_PATH = Path(__file__).parent / 'data/metrics.db'


def init_tracker_db(conn):
    conn.execute('''
        CREATE TABLE IF NOT EXISTS analysis_snapshots (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp   TEXT NOT NULL,
            overall_util REAL,
            total_power_w REAL,
            n_alerts    INTEGER,
            n_critical  INTEGER,
            n_high      INTEGER,
            alert_types TEXT,   -- JSON list
            summary_json TEXT   -- 전체 summary JSON
        )
    ''')
    conn.execute('''
        CREATE TABLE IF NOT EXISTS recurring_issues (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            issue_type   TEXT NOT NULL,
            gpu_index    INTEGER,
            first_seen   TEXT NOT NULL,
            last_seen    TEXT NOT NULL,
            occurrences  INTEGER DEFAULT 1,
            resolved     INTEGER DEFAULT 0
        )
    ''')
    conn.commit()


def save_snapshot(result: dict):
    """매 분석 결과를 스냅샷으로 저장"""
    conn = sqlite3.connect(str(DB_PATH))
    init_tracker_db(conn)

    summary     = result.get('summary', {})
    alerts      = result.get('alerts', [])
    alert_types = list(set(a['type'] for a in alerts))

    conn.execute('''
        INSERT INTO analysis_snapshots
        (timestamp, overall_util, total_power_w, n_alerts,
         n_critical, n_high, alert_types, summary_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        datetime.now().isoformat(),
        summary.get('overall_util', 0),
        summary.get('total_power_w', 0),
        summary.get('n_alerts', 0),
        summary.get('n_critical', 0),
        summary.get('n_high', 0),
        json.dumps(alert_types),
        json.dumps(summary)
    ))
    conn.commit()

    # 반복 문제 업데이트
    _update_recurring(conn, alerts)
    conn.close()


def _update_recurring(conn, alerts: list):
    """같은 문제가 반복되는지 추적"""
    now = datetime.now().isoformat()

    for alert in alerts:
        issue_type = alert['type']
        gpu_index  = alert.get('gpu_index', -1)

        existing = conn.execute('''
            SELECT id, occurrences FROM recurring_issues
            WHERE issue_type = ? AND gpu_index = ? AND resolved = 0
        ''', (issue_type, gpu_index)).fetchone()

        if existing:
            conn.execute('''
                UPDATE recurring_issues
                SET last_seen = ?, occurrences = occurrences + 1
                WHERE id = ?
            ''', (now, existing[0]))
        else:
            conn.execute('''
                INSERT INTO recurring_issues
                (issue_type, gpu_index, first_seen, last_seen, occurrences)
                VALUES (?, ?, ?, ?, 1)
            ''', (issue_type, gpu_index, now, now))

    conn.commit()


def get_changes(result: dict) -> dict:
    """
    이전 스냅샷 대비 변화 계산
    반환: 변화 딕셔너리
    """
    conn = sqlite3.connect(str(DB_PATH))
    init_tracker_db(conn)

    # 이전 스냅샷 (현재 제외 가장 최근)
    prev = conn.execute('''
        SELECT overall_util, total_power_w, n_alerts, alert_types
        FROM analysis_snapshots
        ORDER BY timestamp DESC
        LIMIT 1 OFFSET 1
    ''').fetchone()

    conn.close()

    if not prev:
        return {}

    summary = result.get('summary', {})
    current_util  = summary.get('overall_util', 0)
    current_power = summary.get('total_power_w', 0)
    current_alerts = summary.get('n_alerts', 0)

    prev_util, prev_power, prev_alerts, prev_types_json = prev
    prev_types = json.loads(prev_types_json or '[]')

    current_types = list(set(a['type'] for a in result.get('alerts', [])))
    new_issues    = [t for t in current_types if t not in prev_types]
    resolved      = [t for t in prev_types if t not in current_types]

    return {
        'util_delta'   : round(current_util - prev_util, 1),
        'power_delta'  : round(current_power - prev_power, 1),
        'alerts_delta' : current_alerts - prev_alerts,
        'new_issues'   : new_issues,
        'resolved'     : resolved,
    }


def get_recurring(min_occurrences=3) -> list:
    """N번 이상 반복된 문제 반환"""
    conn = sqlite3.connect(str(DB_PATH))
    init_tracker_db(conn)

    rows = conn.execute('''
        SELECT issue_type, gpu_index, first_seen, last_seen, occurrences
        FROM recurring_issues
        WHERE occurrences >= ? AND resolved = 0
        ORDER BY occurrences DESC
    ''', (min_occurrences,)).fetchall()
    conn.close()

    return [
        {
            'type'       : r[0],
            'gpu_index'  : r[1],
            'first_seen' : r[2][:16],
            'last_seen'  : r[3][:16],
            'occurrences': r[4],
            'message'    : (
                f"{r[0]} on GPU-{r[1]}: "
                f"seen {r[4]}x since {r[2][:10]}"
            )
        }
        for r in rows
    ]


def mark_resolved(issue_type: str, gpu_index: int):
    """문제 해결됨으로 표시"""
    conn = sqlite3.connect(str(DB_PATH))
    conn.execute('''
        UPDATE recurring_issues
        SET resolved = 1
        WHERE issue_type = ? AND gpu_index = ?
    ''', (issue_type, gpu_index))
    conn.commit()
    conn.close()


def print_changes(changes: dict, recurring: list):
    """변화 요약 출력"""
    if not changes:
        print('📊 변화 추적: 첫 번째 실행 (이전 데이터 없음)')
        return

    print('📊 이전 대비 변화:')

    util_delta  = changes.get('util_delta', 0)
    power_delta = changes.get('power_delta', 0)
    alert_delta = changes.get('alerts_delta', 0)

    util_icon  = '📈' if util_delta > 0 else ('📉' if util_delta < 0 else '➡️')
    power_icon = '⚡' if power_delta > 0 else ('💚' if power_delta < 0 else '➡️')

    print(f'  {util_icon} Util:   {util_delta:+.1f}%')
    print(f'  {power_icon} Power:  {power_delta:+.0f}W')
    print(f'  {"🔺" if alert_delta > 0 else "🔻"} Alerts: {alert_delta:+d}')

    if changes.get('new_issues'):
        print(f'  🆕 새 문제: {", ".join(changes["new_issues"])}')
    if changes.get('resolved'):
        print(f'  ✅ 해결됨: {", ".join(changes["resolved"])}')

    if recurring:
        print(f'\n🔁 반복 문제 ({len(recurring)}개):')
        for r in recurring:
            print(f'  ⚠️  {r["message"]}')
