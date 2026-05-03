#!/bin/bash
# InfraLens Agent — One-line installer
# curl -sSL https://raw.githubusercontent.com/SamJeong7201/Infralens/main/infralens_agent/install.sh | bash

set -e

echo "⚡ InfraLens Agent Installer"
echo "============================="

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
echo "✅ Python $PY_VERSION"

# nvidia-smi 확인
if command -v nvidia-smi &>/dev/null; then
    GPU_COUNT=$(nvidia-smi --query-gpu=name --format=csv,noheader | wc -l)
    echo "✅ NVIDIA GPU detected: $GPU_COUNT GPU(s)"
else
    echo "⚠️  nvidia-smi not found — running in test mode"
fi

# 디렉토리 생성
INSTALL_DIR="$HOME/infralens-agent"
mkdir -p "$INSTALL_DIR/data/reports"
mkdir -p "$INSTALL_DIR/analyze"
mkdir -p "$INSTALL_DIR/notify"
cd "$INSTALL_DIR"
echo "✅ Install dir: $INSTALL_DIR"

# 파일 다운로드
BASE="https://raw.githubusercontent.com/SamJeong7201/Infralens/main/infralens_agent"
echo ""
echo "Downloading files..."

# 메인 파일
for f in collect.py run.py execute.py tracker.py env_detect.py config.yaml fake_nvidia_smi.py; do
    echo "  → $f"
    curl -sSL "$BASE/$f" -o "$f"
done

# analyze 폴더
for f in __init__.py anomaly.py idle.py memory.py zombie.py balance.py power.py; do
    echo "  → analyze/$f"
    curl -sSL "$BASE/analyze/$f" -o "analyze/$f"
done

# notify 폴더
for f in __init__.py email.py slack.py teams.py; do
    echo "  → notify/$f"
    curl -sSL "$BASE/notify/$f" -o "notify/$f" 2>/dev/null || true
done

# 패키지 설치
echo ""
echo "Installing dependencies..."
$PYTHON -m pip install pyyaml pandas numpy --quiet
echo "✅ Dependencies installed"

# 환경 감지
echo ""
echo "Detecting environment..."
$PYTHON env_detect.py

# 테스트 실행
echo ""
echo "Running test..."
$PYTHON run.py

echo ""
echo "============================="
echo "✅ Installation complete!"
echo ""
echo "Next steps:"
echo "  1. Edit $INSTALL_DIR/config.yaml"
echo "     - Set lab.name"
echo "     - Set notifications (slack_webhook or email)"
echo "     - Set execution.enabled: true when ready"
echo ""
echo "  2. Run once:"
echo "     cd $INSTALL_DIR && python3 run.py"
echo ""
echo "  3. Run with auto-fix:"
echo "     cd $INSTALL_DIR && python3 run.py --auto"
echo ""
echo "  4. Run continuously (every 5 min):"
echo "     cd $INSTALL_DIR && python3 run.py --loop --auto"
echo ""
echo "  5. Open dashboard:"
echo "     cd $INSTALL_DIR && streamlit run dashboard.py"
echo ""
echo "  6. Schedule with cron (every hour):"
echo "     crontab -e"
echo "     # Add: 0 * * * * cd $INSTALL_DIR && python3 run.py --auto"
