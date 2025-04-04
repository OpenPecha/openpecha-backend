#!/bin/bash
set -e

CONFIG_PATH="public/config.json"
API_URL=$(firebase functions:config:get public.api_url | sed 's/"//g')

[ -z "$API_URL" ] && { echo "Error: Could not retrieve API URL"; exit 1; }
[ ! -f "$CONFIG_PATH" ] && { echo "Error: $CONFIG_PATH not found"; exit 1; }

CONFIG_CONTENT=$(cat "$CONFIG_PATH")
NEW_CONTENT=$(echo "$CONFIG_CONTENT" | sed -E 's|"apiEndpoint": "[^"]*"|"apiEndpoint": "'"$API_URL"'"|')
echo "$NEW_CONTENT" > "$CONFIG_PATH"

echo "Updated apiEndpoint to: $API_URL"
