"""
notify/email.py
───────────────
역할: 이메일 알림
      1) 관리자 — 문제 발생 즉시
      2) 매니저 — 주간 PDF 보고서

보안: 요약본만 전송, raw 데이터 없음
"""
import smtplib
import sqlite3
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from datetime import datetime, timedelta
from pathlib import Path
import yaml

CONFIG_PATH = Path(__file__).parent.parent / 'config.yaml'
DB_PATH     = Path(__file__).parent.parent / 'data/metrics.db'


def load_config():
    with open(CONFIG_PATH) as f:
        return yaml.safe_load(f)


def _send(smtp_cfg: dict, to: str, subject: str, html: str, pdf_bytes=None):
    """
    이메일 발송
    TLS 암호화 사용
    """
    if not smtp_cfg.get('user') or not smtp_cfg.get('password'):
        print('SMTP 설정 없음 — config.yaml에서 smtp 설정 필요')
        return False

    if not to:
        print('수신자 이메일 없음')
        return False

    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From']    = smtp_cfg['user']
    msg['To']      = to

    msg.attach(MIMEText(html, 'html'))

    # PDF 첨부
    if pdf_bytes:
        attachment = MIMEApplication(pdf_bytes, _subtype='pdf')
        attachment.add_header(
            'Content-Disposition', 'attachment',
            filename=f'infralens_report_{datetime.now().strftime("%Y%m%d")}.pdf'
        )
        msg.attach(attachment)

    try:
        with smtplib.SMTP(smtp_cfg['host'], smtp_cfg['port']) as server:
            server.starttls()
            server.login(smtp_cfg['user'], smtp_cfg['password'])
            server.sendmail(smtp_cfg['user'], to, msg.as_string())
        print(f'✅ 이메일 발송 완료 → {to}')
        return True
    except Exception as e:
        print(f'❌ 이메일 발송 실패: {e}')
        return False


def _admin_html(summary: dict, alerts: list, recs: list, lab_name: str) -> str:
    """관리자용 이메일 HTML — 문제 중심"""
    now = datetime.now().strftime('%Y-%m-%d %H:%M')

    # severity별 색상
    sev_color = {'critical': '#dc2626', 'high': '#ea580c',
                 'medium': '#ca8a04', 'low': '#16a34a'}

    alerts_html = ''
    for a in alerts[:10]:  # 최대 10개
        color = sev_color.get(a['severity'], '#6b7280')
        alerts_html += f"""
        <tr>
            <td style="padding:8px;border-bottom:1px solid #e5e7eb;">
                <span style="color:{color};font-weight:bold;">
                    [{a['severity'].upper()}]
                </span>
            </td>
            <td style="padding:8px;border-bottom:1px solid #e5e7eb;">
                {a['type']}
            </td>
            <td style="padding:8px;border-bottom:1px solid #e5e7eb;font-size:13px;">
                {a['message']}
            </td>
        </tr>"""

    recs_html = ''
    for r in recs[:5]:  # 최대 5개
        auto = '✅ 자동 안전' if r.get('auto_safe') else '⚠️ 수동 확인'
        recs_html += f"""
        <tr>
            <td style="padding:8px;border-bottom:1px solid #e5e7eb;">{auto}</td>
            <td style="padding:8px;border-bottom:1px solid #e5e7eb;font-size:13px;">
                {r['message']}
            </td>
            <td style="padding:8px;border-bottom:1px solid #e5e7eb;">
                <code style="background:#f3f4f6;padding:2px 6px;border-radius:3px;font-size:12px;">
                    {r.get('command','').split(chr(10))[0]}
                </code>
            </td>
        </tr>"""

    return f"""
    <html><body style="font-family:Arial,sans-serif;max-width:700px;margin:0 auto;color:#111;">
        <div style="background:#1e1b4b;padding:20px;border-radius:8px 8px 0 0;">
            <h1 style="color:white;margin:0;font-size:22px;">⚡ InfraLens Alert</h1>
            <p style="color:#a5b4fc;margin:4px 0 0;">{lab_name} — {now}</p>
        </div>

        <div style="background:#fef2f2;padding:16px;border-left:4px solid #dc2626;">
            <strong>🚨 {len(alerts)}개 문제 감지
            ({summary.get('n_critical',0)} critical,
             {summary.get('n_high',0)} high)</strong>
        </div>

        <div style="padding:20px;background:#f9fafb;">
            <table style="width:100%;border-collapse:collapse;">
                <tr style="background:#e5e7eb;">
                    <th style="padding:8px;text-align:left;">심각도</th>
                    <th style="padding:8px;text-align:left;">유형</th>
                    <th style="padding:8px;text-align:left;">내용</th>
                </tr>
                {alerts_html}
            </table>
        </div>

        <div style="padding:20px;">
            <h3>💡 권장사항</h3>
            <table style="width:100%;border-collapse:collapse;">
                <tr style="background:#e5e7eb;">
                    <th style="padding:8px;text-align:left;">유형</th>
                    <th style="padding:8px;text-align:left;">내용</th>
                    <th style="padding:8px;text-align:left;">명령어</th>
                </tr>
                {recs_html}
            </table>
        </div>

        <div style="padding:16px;background:#f3f4f6;font-size:12px;color:#6b7280;
                    border-radius:0 0 8px 8px;text-align:center;">
            📍 요약본만 전송됨 — 원시 데이터는 서버 내부에만 저장
        </div>
    </body></html>"""


def _manager_html(summary: dict, changes: dict, lab_name: str) -> str:
    """매니저용 이메일 HTML — 비용/효율 중심"""
    now   = datetime.now().strftime('%Y-%m-%d')
    week  = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')

    util  = summary.get('overall_util', 0)
    power = summary.get('total_power_w', 0)

    # 절감 가능 비용 추정
    idle_pct     = summary.get('idle_pct', 0)
    monthly_cost = power * 24 * 30 / 1000 * 0.12
    potential    = monthly_cost * (idle_pct / 100) * 0.7

    util_delta  = changes.get('util_delta', 0)
    power_delta = changes.get('power_delta', 0)

    return f"""
    <html><body style="font-family:Arial,sans-serif;max-width:700px;margin:0 auto;color:#111;">
        <div style="background:#1e1b4b;padding:20px;border-radius:8px 8px 0 0;">
            <h1 style="color:white;margin:0;font-size:22px;">⚡ InfraLens 주간 보고서</h1>
            <p style="color:#a5b4fc;margin:4px 0 0;">
                {lab_name} — {week} ~ {now}
            </p>
        </div>

        <div style="padding:20px;display:flex;gap:16px;">
            <div style="flex:1;background:#f0fdf4;padding:16px;border-radius:8px;text-align:center;">
                <div style="font-size:32px;font-weight:bold;color:#16a34a;">{util}%</div>
                <div style="color:#6b7280;font-size:14px;">평균 GPU 사용률</div>
                <div style="font-size:12px;color:{'#16a34a' if util_delta > 0 else '#dc2626'};">
                    {'+' if util_delta > 0 else ''}{util_delta}% 전주 대비
                </div>
            </div>
            <div style="flex:1;background:#fff7ed;padding:16px;border-radius:8px;text-align:center;">
                <div style="font-size:32px;font-weight:bold;color:#ea580c;">${potential:.0f}</div>
                <div style="color:#6b7280;font-size:14px;">절감 가능 비용/월</div>
            </div>
            <div style="flex:1;background:#fef2f2;padding:16px;border-radius:8px;text-align:center;">
                <div style="font-size:32px;font-weight:bold;color:#dc2626;">
                    {summary.get('n_alerts',0)}
                </div>
                <div style="color:#6b7280;font-size:14px;">감지된 문제</div>
            </div>
        </div>

        <div style="padding:0 20px 20px;">
            <h3>📊 전주 대비 변화</h3>
            <table style="width:100%;border-collapse:collapse;">
                <tr style="background:#f3f4f6;">
                    <td style="padding:10px;">GPU 사용률</td>
                    <td style="padding:10px;font-weight:bold;
                        color:{'#16a34a' if util_delta > 0 else '#dc2626'};">
                        {'+' if util_delta > 0 else ''}{util_delta}%
                    </td>
                </tr>
                <tr>
                    <td style="padding:10px;">전력 소비</td>
                    <td style="padding:10px;font-weight:bold;
                        color:{'#dc2626' if power_delta > 0 else '#16a34a'};">
                        {'+' if power_delta > 0 else ''}{power_delta:.0f}W
                    </td>
                </tr>
            </table>
        </div>

        <div style="padding:16px;background:#f3f4f6;font-size:12px;color:#6b7280;
                    border-radius:0 0 8px 8px;text-align:center;">
            📍 요약본만 전송됨 — 원시 데이터는 서버 내부에만 저장<br>
            자세한 내용은 첨부 PDF를 확인하세요
        </div>
    </body></html>"""


def send_admin_alert(result: dict):
    """관리자에게 즉시 알림"""
    config   = load_config()
    smtp_cfg = config['notifications']['smtp']
    to       = config['notifications']['admin']['email']
    lab_name = config['lab']['name']

    alerts = result.get('alerts', [])
    recs   = result.get('recommendations', [])
    summary = result.get('summary', {})

    # critical/high 있을 때만 발송
    if not any(a['severity'] in ['critical','high'] for a in alerts):
        print('관리자 알림 스킵 — critical/high 없음')
        return

    subject = (f"[InfraLens] 🚨 {summary.get('n_critical',0)} critical, "
               f"{summary.get('n_high',0)} high alerts — {lab_name}")
    html    = _admin_html(summary, alerts, recs, lab_name)

    _send(smtp_cfg, to, subject, html)


def send_manager_report(result: dict, changes: dict, pdf_bytes=None):
    """매니저에게 주간 보고서 발송"""
    config   = load_config()
    smtp_cfg = config['notifications']['smtp']
    to       = config['notifications']['manager']['email']
    lab_name = config['lab']['name']

    summary = result.get('summary', {})
    subject = f"[InfraLens] 📊 주간 GPU 보고서 — {lab_name}"
    html    = _manager_html(summary, changes, lab_name)

    _send(smtp_cfg, to, subject, html, pdf_bytes=pdf_bytes)


if __name__ == '__main__':
    # 테스트 — 실제 발송 없이 HTML만 확인
    test_summary = {
        'n_gpus': 8, 'overall_util': 34.1,
        'idle_pct': 62.5, 'total_power_w': 1269.0,
        'n_alerts': 5, 'n_critical': 0, 'n_high': 3, 'n_medium': 2
    }
    test_alerts = [
        {'severity': 'high', 'type': 'ZOMBIE_PROCESS',
         'message': 'GPU-5: 1% util but 75% VRAM occupied'},
        {'severity': 'high', 'type': 'MEMORY_LEAK',
         'message': 'GPU-4: VRAM growing +800MB/h'},
    ]
    test_recs = [
        {'auto_safe': False, 'message': 'GPU-5 zombie process',
         'command': 'nvidia-smi -i 5 --query-compute-apps=pid'},
    ]
    test_changes = {'util_delta': -2.1, 'power_delta': +45.0}

    html = _admin_html(test_summary, test_alerts, test_recs, 'Test Lab')
    Path('data/test_admin_email.html').write_text(html)
    print('✅ data/test_admin_email.html 생성됨 — 브라우저로 열어서 확인')

    html2 = _manager_html(test_summary, test_changes, 'Test Lab')
    Path('data/test_manager_email.html').write_text(html2)
    print('✅ data/test_manager_email.html 생성됨 — 브라우저로 열어서 확인')
