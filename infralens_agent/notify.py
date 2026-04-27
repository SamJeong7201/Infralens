"""
notify.py
─────────
역할: Slack/이메일 알림
      원시 데이터는 안 보내고 요약만 전송
"""
import json
import urllib.request
import urllib.error
from datetime import datetime
from pathlib import Path
import yaml

CONFIG_PATH = Path(__file__).parent / 'config.yaml'

def load_config():
    with open(CONFIG_PATH) as f:
        return yaml.safe_load(f)


def send_slack(webhook_url: str, message: dict):
    """Slack webhook으로 알림 전송"""
    if not webhook_url:
        print('Slack webhook not configured')
        return False

    payload = json.dumps(message).encode('utf-8')
    req = urllib.request.Request(
        webhook_url,
        data=payload,
        headers={'Content-Type': 'application/json'}
    )
    try:
        urllib.request.urlopen(req, timeout=10)
        return True
    except Exception as e:
        print(f'Slack error: {e}')
        return False


def format_alert_message(lab_name: str, state: dict,
                          alerts: list, recs: list) -> dict:
    """Slack 메시지 포맷 - 요약만, 원시 데이터 없음"""
    now = datetime.now().strftime('%Y-%m-%d %H:%M')

    # 상태 이모지
    util = state.get('overall_util', 0)
    if util < 20:
        status_emoji = '🔴'
        status_text  = 'Critical — very low utilization'
    elif util < 50:
        status_emoji = '🟡'
        status_text  = 'Warning — below optimal'
    else:
        status_emoji = '🟢'
        status_text  = 'Good'

    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text",
                     "text": f"⚡ InfraLens Report — {lab_name}"}
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*Time*\n{now}"},
                {"type": "mrkdwn", "text": f"*Status*\n{status_emoji} {status_text}"},
                {"type": "mrkdwn", "text": f"*Cluster Util*\n{util}%"},
                {"type": "mrkdwn", "text": f"*Idle GPUs*\n{state.get('n_idle_now', 0)} / {state.get('n_gpus', 0)}"},
                {"type": "mrkdwn", "text": f"*Total Power*\n{state.get('total_power_w', 0)}W"},
                {"type": "mrkdwn", "text": f"*Idle Power*\n{state.get('idle_power_w', 0)}W"},
            ]
        }
    ]

    # 알림 추가
    if alerts:
        alert_text = '\n'.join([
            f"{'🚨' if a['severity']=='critical' else '⚠️'} {a['message']}"
            for a in alerts[:5]
        ])
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn",
                     "text": f"*Alerts ({len(alerts)})*\n{alert_text}"}
        })

    # 권장사항 추가
    if recs:
        rec_text = '\n'.join([
            f"💡 {r['title']}"
            for r in recs[:3]
        ])
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn",
                     "text": f"*Recommendations*\n{rec_text}"}
        })

    # 참고: 원시 데이터는 전송 안 함
    blocks.append({
        "type": "context",
        "elements": [{
            "type": "mrkdwn",
            "text": "📍 Data stays on your server — only this summary was sent"
        }]
    })

    return {"blocks": blocks}


def run_notify(state: dict, alerts: list, recs: list):
    config   = load_config()
    lab_name = config['lab']['name']
    webhook  = config['notifications']['slack_webhook']

    if not webhook:
        print('No Slack webhook configured in config.yaml')
        print('Add: notifications.slack_webhook: https://hooks.slack.com/...')
        return

    message = format_alert_message(lab_name, state, alerts, recs)
    success = send_slack(webhook, message)

    if success:
        print('✅ Slack notification sent')
    else:
        print('❌ Slack notification failed')


if __name__ == '__main__':
    # 테스트용
    test_state = {
        'overall_util': 27,
        'n_gpus': 12,
        'n_idle_now': 8,
        'total_power_w': 2400,
        'idle_power_w': 800,
    }
    test_alerts = [
        {'severity': 'high', 'message': 'GPU-3 idle for 6+ hours at 2.1% util'}
    ]
    test_recs = [
        {'title': '8 GPUs idle — power limiting saves ~$140/month'}
    ]
    run_notify(test_state, test_alerts, test_recs)
