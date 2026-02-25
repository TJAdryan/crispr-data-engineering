#!/bin/bash

# 1. Spark & Java 17+ Environment Configuration
# This handles the Delta Lake packages and the Java memory access requirements
export PYSPARK_SUBMIT_ARGS="--packages io.delta:delta-spark_2.12:3.1.0 --conf spark.driver.extraJavaOptions='--add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED' --conf spark.executor.extraJavaOptions='--add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED' pyspark-shell"

# 2. Network and Port Configuration
LOCAL_IP=$(hostname -I | awk '{print $1}')
PORT=8888

# 3. Fedora Firewall Check
# Ensures the port is actually reachable from your local network
if command -v firewall-cmd >/dev/null 2>&1; then
    # Check if the port is already open
    IS_OPEN=$(sudo firewall-cmd --query-port=$PORT/tcp)
    if [ "$IS_OPEN" = "no" ]; then
        echo "Opening port $PORT in Fedora firewall..."
        sudo firewall-cmd --add-port=$PORT/tcp --permanent
        sudo firewall-cmd --reload
    else
        echo "Port $PORT is already open in firewall."
    fi
fi

echo "-------------------------------------------------------"
echo "CRISPR Project: Jupyter Lab Server"
echo "Access from your local machine: http://$LOCAL_IP:$PORT"
echo "-------------------------------------------------------"

# 4. Launch Jupyter via uv
uv run jupyter lab --ip 0.0.0.0 --port $PORT --no-browser