"""
run.py - InfraLens Agent 메인 실행
python run.py           → 한 번 실행
python run.py --loop    → 매 시간 반복
python run.py --notify  → 알림 포함
"""
import sys
import time
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from collect import run_collection
from analyze import run_analysis


def run_once(notify=False):
    print('=' * 50)
    print(f'InfraLens Agent — {datetime.now().strftime("%Y-%m-%d %H:%M")}')
    print('=' * 50)

    run_collection()
    print()

    result = run_analysis()
    print()

    if notify and result:
        try:
            import yaml
            with open('config.yaml') as f:
                config = yaml.safe_load(f)

            channel = config.get('notifications', {}).get('channel', 'stdout')

            if channel == 'slack':
                from notify import SlackNotifier
                notifier = SlackNotifier(config['notifications']['slack'])
            elif channel == 'teams':
                from notify import TeamsNotifier
                notifier = TeamsNotifier(config['notifications']['teams'])
            elif channel == 'email':
                from notify import EmailNotifier
                notifier = EmailNotifier(config['notifications']['email'])
            else:
                print('Notification summary:')
                print(f'  Util: {result["state"].get("overall_util",0)}%')
                print(f'  Idle: {result["state"].get("n_idle_now",0)} GPUs')
                print(f'  Alerts: {len(result["alerts"])}')
                return

            from notify import NotifyPayload, Alert
            payload = NotifyPayload(
                lab_name=config['lab']['name'],
                timestamp=datetime.now().strftime('%Y-%m-%d %H:%M'),
                overall_util=result['state'].get('overall_util', 0),
                n_gpus=result['state'].get('n_gpus', 0),
                n_idle=result['state'].get('n_idle_now', 0),
                total_power_w=result['state'].get('total_power_w', 0),
                idle_power_w=result['state'].get('idle_power_w', 0),
                alerts=[Alert(**a) for a in result['alerts']],
                recommendations=result['recommendations'],
            )
            notifier.send(payload)
        except Exception as e:
            print(f'Notify error: {e}')


def run_loop(interval_minutes=60, notify=False):
    print(f'InfraLens Agent — running every {interval_minutes} minutes')
    print('Press Ctrl+C to stop')
    print()
    while True:
        try:
            run_once(notify=notify)
            print(f'\nNext run in {interval_minutes} minutes...\n')
            time.sleep(interval_minutes * 60)
        except KeyboardInterrupt:
            print('\nStopped.')
            break


if __name__ == '__main__':
    loop   = '--loop'   in sys.argv
    notify = '--notify' in sys.argv

    if loop:
        import yaml
        with open('config.yaml') as f:
            config = yaml.safe_load(f)
        run_loop(config['collection']['interval_minutes'], notify=notify)
    else:
        run_once(notify=notify)
