#!/bin/bash

# Wait for Kafka to start
sleep 10

# Create the Kafka topic
kafka-topics --create --topic test-topic --bootstrap-server kafka:9092 --partitions 1 --replication-factor 1 || true

# Stream JSON objects in an infinite loop
if [ -f /files/idnes.json ]; then
    while true; do  # Infinite loop to continuously read the file
        echo "Restarting the JSON stream..."
        
        # Use `jq` to extract JSON objects and stream them
        jq -c '.[]' /files/idnes.json | while IFS= read -r json_line; do
            echo "$json_line" | kafka-console-producer --broker-list kafka:9092 --topic test-topic
            echo "Produced: $json_line"
            # Generate a random sleep interval between 1 and 15 seconds
            sleep_interval=$((RANDOM % 10 + 1))
            echo "Sleeping for $sleep_interval seconds..."
            sleep "$sleep_interval"
        done
    done
else
    echo "No JSON data file found at /files/idnes.json"
    while :; do sleep 10; done  # Keep the container running if the file is missing
fi
