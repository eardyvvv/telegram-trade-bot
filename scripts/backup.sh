#!/bin/bash
# Daily database backup — keeps 7 days
# Run via cron daily at 5AM UTC

DB_PATH="/home/botadmin/trading-bot/data/bot.db"
BACKUP_DIR="/home/botadmin/trading-bot/data/backups"
KEEP_DAYS=7

mkdir -p "$BACKUP_DIR"

DATE=$(date +%Y-%m-%d)
BACKUP_FILE="${BACKUP_DIR}/bot_${DATE}.db"

# Use SQLite backup command for safe copy
sqlite3 "$DB_PATH" ".backup '${BACKUP_FILE}'"

if [ $? -eq 0 ]; then
    echo "$(date): Backup created: ${BACKUP_FILE}" >> /home/botadmin/trading-bot/logs/backup.log
else
    echo "$(date): Backup FAILED" >> /home/botadmin/trading-bot/logs/backup.log
fi

# Delete backups older than 7 days
find "$BACKUP_DIR" -name "bot_*.db" -mtime +${KEEP_DAYS} -delete
