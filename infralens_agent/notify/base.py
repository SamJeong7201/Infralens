from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import List


@dataclass
class Alert:
    severity: str
    message: str
    gpu: str = ''
    metric: float = 0.0


@dataclass
class NotifyPayload:
    lab_name:        str
    timestamp:       str
    overall_util:    float
    n_gpus:          int
    n_idle:          int
    total_power_w:   float
    idle_power_w:    float
    alerts:          List[Alert] = field(default_factory=list)
    recommendations: List[dict]  = field(default_factory=list)


class BaseNotifier(ABC):

    @abstractmethod
    def send(self, payload: NotifyPayload) -> bool:
        pass

    def format_summary(self, payload: NotifyPayload) -> str:
        lines = [
            f'InfraLens Report — {payload.lab_name}',
            f'Time: {payload.timestamp}',
            f'',
            f'Cluster: {payload.overall_util}% util | '
            f'{payload.n_idle}/{payload.n_gpus} GPUs idle | '
            f'{payload.total_power_w}W total power',
        ]
        if payload.alerts:
            lines += ['', f'Alerts ({len(payload.alerts)}):']
            for a in payload.alerts[:5]:
                icon = '!!' if a.severity == 'critical' else '!'
                lines.append(f'  {icon} {a.message}')
        if payload.recommendations:
            lines += ['', f'Recommendations:']
            for r in payload.recommendations[:3]:
                lines.append(f'  -> {r["title"]}')
        lines += ['', 'Data stays on your server — only this summary was sent']
        return '\n'.join(lines)
