#!/bin/bash
# ==============================================================================
# Brookfield PE Intelligence Bot - 24/7 Google Cloud VM Installer
# ==============================================================================
set -e

echo "=========================================================="
echo "🚀 Installing Brookfield PE Intelligence Bot 24/7 Service"
echo "=========================================================="

# 1. Install system packages
echo "📦 Updating packages and installing Python 3 + venv..."
sudo apt-get update -y
sudo apt-get install -y python3 python3-pip python3-venv unzip

# 2. Setup Virtual Environment
echo "🐍 Setting up Python virtual environment..."
if [ ! -d ".venv" ]; then
    python3 -m venv .venv
fi
source .venv/bin/activate

echo "📦 Installing Python dependencies..."
pip install --upgrade pip
pip install -r requirements.txt

# 3. Configure systemd service
CURRENT_USER=$(whoami)
CURRENT_DIR=$(pwd)
SERVICE_FILE="/etc/systemd/system/brookfield-bot.service"

echo "⚙️ Creating systemd 24/7 background service at ${SERVICE_FILE}..."
cat <<EOF | sudo tee ${SERVICE_FILE}
[Unit]
Description=Brookfield PE Intelligence Telegram Bot
After=network.target

[Service]
Type=simple
User=${CURRENT_USER}
WorkingDirectory=${CURRENT_DIR}
ExecStart=${CURRENT_DIR}/.venv/bin/python ${CURRENT_DIR}/interactive_bot.py --interval 3600
Restart=always
RestartSec=10
Environment=PYTHONUNBUFFERED=1

[Install]
WantedBy=multi-user.target
EOF

# 4. Enable and launch service
echo "🔄 Reloading systemd daemon and starting brookfield-bot service..."
sudo systemctl daemon-reload
sudo systemctl enable brookfield-bot
sudo systemctl restart brookfield-bot

echo ""
echo "=========================================================="
echo "✅ SETUP COMPLETE! The bot is now running 24/7 in the cloud."
echo "=========================================================="
echo "Useful commands:"
echo "• Check status:  sudo systemctl status brookfield-bot"
echo "• View live log: journalctl -u brookfield-bot -f"
echo "• Stop bot:      sudo systemctl stop brookfield-bot"
echo "• Restart bot:   sudo systemctl restart brookfield-bot"
echo "=========================================================="
