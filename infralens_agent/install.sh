#!/bin/bash
# InfraLens Agent — One-line installer
# curl -sSL https://raw.githubusercontent.com/.../install.sh | bash

set -e

echo "InfraLens Agent Installer"
echo "========================="

# Python 확인
if command -v python3 &>/dev/null; then
    PYTHON=python3
elif command -v python &>/dev/null; then
    PYTHON=python
else
    echo "ERROR: Python not found. Please install Python 3.8+"
    exit 1
fi

PY_VERSION=$($PYTHON -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
echo "Python: $PY_VERSION"

# 디렉토리 생성
mkdir -p ~/infralens-agent/data/reports
cd ~/infralens-agent

# 파일 다운로드 (GitHub raw)
BASE_URL="https://raw.githubusercontent.com/SamJeong7201/Infralens/main/infralens_agent"
for f in collect.py analyze.py execute.py run.py config.yaml; do
    echo "Downloading $f..."
    curl -sSL "$BASE_URL/$f" -o "$f"
done

mkdir -p notify
for f in __init__.py base.py email.py slack.py teams.py; do
    curl -sSL "$BASE_URL/notify/$f" -o "notify/$f"
done

# 패키지 설치
echo "Installing dependencies..."
$PYTHON -m pip install pyyaml pandas --quiet

# 테스트 실행
echo ""
echo "Testing..."
$PYTHON run.py

echo ""
echo "========================="
echo "Installation complete!"
echo ""
echo "Next steps:"
echo "  1. Edit ~/infralens-agent/config.yaml"
echo "     - Set lab.name"
echo "     - Set notifications (email/slack)"
echo ""
echo "  2. Run manually:"
echo "     cd ~/infralens-agent && python3 run.py"
echo ""
echo "  3. Schedule (runs every hour):"
echo "     crontab -e"
echo "     # Add: 0 * * * * cd ~/infralens-agent && python3 run.py --notify"
