#!/bin/sh
set -e

# Install lightweight dependencies
apk add --no-cache curl jq > /dev/null 2>&1

echo "Waiting for Kafka Connect REST API..."
until curl -s http://kafka-connect:8083/connectors > /dev/null; do
  sleep 2
done

echo "Auto-registering all connectors in /connectors..."
for file in /connectors/*.json; do
  if [ -f "$file" ]; then
    NAME=$(jq -r '.name' "$file")
    echo "Processing connector: $NAME from $file"
    
    # Extract config and register/update via PUT
    jq '.config' "$file" | curl -s -X PUT "http://kafka-connect:8083/connectors/$NAME/config" \
      -H 'Content-Type: application/json' \
      -d @- > /dev/null
      
    echo "Connector '$NAME' registered/updated successfully!"
  fi
done

echo ""
echo "Current active connectors:"
curl -s http://kafka-connect:8083/connectors | jq .