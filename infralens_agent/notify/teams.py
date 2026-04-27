import json, urllib.request
from .base import BaseNotifier, NotifyPayload


class TeamsNotifier(BaseNotifier):

    def __init__(self, config: dict):
        self.url = config.get('webhook_url', '')

    def send(self, payload: NotifyPayload) -> bool:
        if not self.url:
            print('Teams: no webhook_url in config.yaml')
            return False
        try:
            data = json.dumps(self._card(payload)).encode()
            req  = urllib.request.Request(self.url, data=data,
                                          headers={'Content-Type':'application/json'})
            urllib.request.urlopen(req, timeout=10)
            print('Teams notification sent')
            return True
        except Exception as e:
            print(f'Teams failed: {e}')
            return False

    def _card(self, p):
        facts = [
            {"name":"Utilization","value":f"{p.overall_util}%"},
            {"name":"Idle GPUs",  "value":f"{p.n_idle}/{p.n_gpus}"},
            {"name":"Idle Power", "value":f"{p.idle_power_w}W"},
        ]
        for a in p.alerts[:3]:
            facts.append({"name":"Alert","value":a.message})
        return {
            "@type":"MessageCard","@context":"http://schema.org/extensions",
            "summary":f"InfraLens — {p.lab_name}","themeColor":"6366f1",
            "title":f"InfraLens — {p.lab_name}",
            "sections":[{"facts":facts,
                         "text":"Data stays on your server — only this summary was sent"}]
        }
