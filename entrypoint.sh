#!/bin/bash

# Start the Spark master and worker
$SPARK_HOME/sbin/start-master.sh

# Check if the Spark Master is running
while ! nc -z localhost 7077; do
  echo "Waiting for Spark Master to start..."
  sleep 1
done

$SPARK_HOME/sbin/start-worker.sh spark://$(hostname):7077

# Keep the container running
tail -f /dev/null