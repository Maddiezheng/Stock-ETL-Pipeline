#!/bin/sh

cd /opt/spark/work-dir

echo "Initializing delta table..."
python3 ./stocklake/delta_tables/create_bronze_layer.py && python3 ./stocklake/delta_tables/create_silver_layer.py

# Tail the log file to keep the container running
tail -f /var/log/cron.log
