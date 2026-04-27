import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from .base import BaseNotifier, NotifyPayload


class EmailNotifier(BaseNotifier):

    def __init__(self, config: dict):
        self.config = config

    def send(self, payload: NotifyPayload) -> bool:
        cfg = self.config
        if not cfg.get('to'):
            print('Email: no recipient configured in config.yaml')
            return False
        try:
            msg = MIMEMultipart('alternative')
            msg['Subject'] = self._subject(payload)
            msg['From']    = cfg.get('username', '')
            msg['To']      = cfg.get('to', '')
            msg.attach(MIMEText(self.format_summary(payload), 'plain'))
            msg.attach(MIMEText(self._html(payload), 'html'))

            with smtplib.SMTP(cfg.get('smtp_host','smtp.gmail.com'),
                              cfg.get('smtp_port', 587)) as s:
                s.starttls()
                s.login(cfg['username'], cfg['password'])
                s.sendmail(cfg['username'], cfg['to'], msg.as_string())
            print(f'Email sent to {cfg["to"]}')
            return True
        except Exception as e:
            print(f'Email failed: {e}')
            return False

    def _subject(self, p):
        if not p.alerts:
            return f'InfraLens Daily Report — {p.lab_name}'
        if any(a.severity == 'critical' for a in p.alerts):
            return f'[ALERT] InfraLens — {p.lab_name} ({len(p.alerts)} issues)'
        return f'[Warning] InfraLens — {p.lab_name} ({len(p.alerts)} issues)'

    def _html(self, p):
        util_color = '#059669' if p.overall_util > 60 else '#d97706' if p.overall_util > 30 else '#dc2626'
        alerts_html = ''.join([
            f'<tr><td style="color:{"#dc2626" if a.severity=="critical" else "#d97706"}">'
            f'{"[!!]" if a.severity=="critical" else "[!]"} {a.message}</td></tr>'
            for a in p.alerts[:5]
        ])
        recs_html = ''.join([
            f'<tr><td>[->] {r["title"]}</td></tr>'
            for r in p.recommendations[:3]
        ])
        return f'''<html><body style="font-family:Arial,sans-serif;max-width:600px;margin:0 auto">
<div style="background:#6366f1;color:white;padding:20px;border-radius:8px 8px 0 0">
  <h2 style="margin:0">InfraLens Report</h2>
  <p style="margin:4px 0;opacity:0.8">{p.lab_name} — {p.timestamp}</p>
</div>
<div style="background:#f9fafb;padding:20px;border:1px solid #e5e7eb">
  <table style="width:100%;border-collapse:collapse;margin-bottom:16px">
    <tr>
      <td style="padding:12px;text-align:center;background:white;border-radius:8px">
        <div style="font-size:11px;color:#6b7280">UTILIZATION</div>
        <div style="font-size:24px;font-weight:bold;color:{util_color}">{p.overall_util}%</div>
      </td>
      <td style="padding:12px;text-align:center;background:white;border-radius:8px">
        <div style="font-size:11px;color:#6b7280">IDLE GPUs</div>
        <div style="font-size:24px;font-weight:bold;color:#dc2626">{p.n_idle}/{p.n_gpus}</div>
      </td>
      <td style="padding:12px;text-align:center;background:white;border-radius:8px">
        <div style="font-size:11px;color:#6b7280">IDLE POWER</div>
        <div style="font-size:24px;font-weight:bold;color:#d97706">{p.idle_power_w}W</div>
      </td>
    </tr>
  </table>
  {"<h3>Alerts</h3><table style='width:100%'>" + alerts_html + "</table>" if alerts_html else ""}
  {"<h3>Recommendations</h3><table style='width:100%'>" + recs_html + "</table>" if recs_html else ""}
  <p style="font-size:11px;color:#9ca3af;margin-top:20px;border-top:1px solid #e5e7eb;padding-top:12px">
    Data stays on your server — only this summary was sent by InfraLens
  </p>
</div></body></html>'''
