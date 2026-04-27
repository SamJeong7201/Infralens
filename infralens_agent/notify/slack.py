import json, urllib.request
from .base import BaseNotifier, NotifyPayload


class SlackNotifier(BaseNotifier):

    def __init__(self, config: dict):
        self.url = config.get('webhook_url', '')

    def send(self, payload: NotifyPayload) -> bool:
        if not self.url:
            print('Slack: no webhook_url in config.yaml')
            return False
        try:
            data = json.dumps(self._blocks(payload)).encode()
            req  = urllib.request.Request(self.url, data=data,
                                          headers={'Content-Type':'application/json'})
            urllib.request.urlopen(req, timeout=10)
            print('Slack notification sent')
            return True
        except Exception as e:
            print(f'Slack failed: {e}')
            return False

    def _blocks(self, p):
        emoji = '[G]' if p.overall_util > 60 else '[Y]' if p.overall_util > 30 else '[R]'
        blocks = [
            {"type":"header","text":{"type":"plain_text","text":f"InfraLens - {p.lab_name}"}},
            {"type":"section","fields":[
                {"type":"mrkdwn","text":f"*Util*\n{emoji} {p.overall_util}%"},
                {"type":"mrkdwn","text":f"*Idle GPUs*\n{p.n_idle}/{p.n_gpus}"},
                {"type":"mrkdwn","text":f"*Idle Power*\n{p.idle_power_w}W"},
                {"type":"mrkdwn","text":f"*Time*\n{p.timestamp}"},
            ]},
        ]
        if p.alerts:
            text = '\n'.join([f'{"[!!]" if a.severity=="critical" else "[!]"} {a.message}' for a in p.alerts[:5]])
            blocks.append({"type":"section","text":{"type":"mrkdwn","text":f"*Alerts*\n{text}"}})
        if p.recommendations:
            text = '\n'.join([f'[->] {r["title"]}' for r in p.recommendations[:3]])
            blocks.append({"type":"section","text":{"type":"mrkdwn","text":f"*Recommendations*\n{text}"}})
        blocks.append({"type":"context","elements":[{"type":"mrkdwn","text":"Data stays on your server — only this summary was sent"}]})
        return {"blocks": blocks}
