#!/bin/bash
# Watchdog script — checks if trading-bot is running
# Run via cron every 5 minutes

SERVICE="trading-bot"
BOT_TOKEN="$(grep TELEGRAM_BOT_TOKEN /home/botadmin/trading-bot/.env | cut -d'=' -f2)"
ADMIN_ID="$(grep TELEGRAM_ADMIN_ID /home/botadmin/trading-bot/.env | cut -d'=' -f2)"
LOG="/home/botadmin/trading-bot/logs/watchdog.log"

send_alert() {
    local msg="$1"
    curl -s -X POST "https://api.telegram.org/bot${BOT_TOKEN}/sendMessage" \
        -d chat_id="${ADMIN_ID}" \
        -d text="🚨 Watchdog: ${msg}" > /dev/null 2>&1
}

# Check if service is running
if ! systemctl is-active --quiet "$SERVICE"; then
    echo "$(date): Service not running, restarting..." >> "$LOG"
    sudo systemctl restart "$SERVICE"
    sleep 5

    if systemctl is-active --quiet "$SERVICE"; then
        echo "$(date): Restart successful" >> "$LOG"
        send_alert "Bot was down, restarted successfully."
    else
        echo "$(date): Restart FAILED" >> "$LOG"
        send_alert "Bot is DOWN and restart failed! Manual intervention needed."
    fi
else
    # Service is running, check if process is responsive
    PID=$(systemctl show -p MainPID --value "$SERVICE")
    if [ "$PID" = "0" ]; then
        echo "$(date): PID is 0, restarting..." >> "$LOG"
        sudo systemctl restart "$SERVICE"
        send_alert "Bot PID was 0, restarted."
    fi
fi
