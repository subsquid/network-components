PORT="${HTTP_LISTEN_ADDR##*:}"

yq '.available_datasets.[] | key' "$CONFIG_PATH" | xargs -I % curl -s -f "http://localhost:$PORT/network/%/height" > /dev/null
