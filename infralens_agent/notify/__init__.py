from .base import BaseNotifier, NotifyPayload, Alert
from .email import EmailNotifier
from .slack import SlackNotifier
from .teams import TeamsNotifier

def get_notifier(config: dict):
    channel = config.get('notifications', {}).get('channel', 'email')
    if channel == 'slack':
        return SlackNotifier(config['notifications'].get('slack', {}))
    elif channel == 'teams':
        return TeamsNotifier(config['notifications'].get('teams', {}))
    else:
        return EmailNotifier(config['notifications'].get('email', {}))
