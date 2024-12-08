#!/bin/bash

# Wait for Kafka to start
sleep 10

# Create the Kafka topic
kafka-topics --create --topic test-topic --bootstrap-server kafka:9092 --partitions 1 --replication-factor 1 || true

# Stream lines in an infinite loop
if [ -f /files/idnes-big.txt ]; then
    while true; do  # Infinite loop to continuously read the file
        echo "Restarting the file stream..."
        while IFS= read -r line; do
            echo "$line" | kafka-console-producer --broker-list kafka:9092 --topic test-topic
            echo "Produced: $line"
            sleep 5  # Delay of 5 seconds between lines
        done < /files/idnes-big.txt
    done
else
    echo "No data file found at /files/idnes-big.txt"
    while :; do sleep 10; done  # Keep the container running if the file is missing
fi