#!/usr/bin/env bash

# Remove whitespace
P2P_LISTEN_ADDRS=$(echo "$P2P_LISTEN_ADDRS" | tr -d '[:space:]')

# Split the string on commas
IFS="," read -r -a ADDRS <<< "$P2P_LISTEN_ADDRS"

# Test all listen ports
for ADDR in "${ADDRS[@]}"; do
  PORT=$(sed -E 's/.*\/(tcp|udp)\/([0-9]+).*/\2/' <<< "$ADDR")
  if ! netstat -lntu | grep "$PORT" > /dev/null
  then
    exit 1
  fi
done

# Test HTTP port
PORT="${HTTP_LISTEN_ADDR##*:}"
if ! curl -s -f "http://localhost:$PORT/metrics" > /dev/null
then
  exit 1
fi
