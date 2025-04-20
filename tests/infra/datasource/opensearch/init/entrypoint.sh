#!/bin/bash
set -e

INIT_DIR="/usr/share/opensearch/docker-entrypoint-initdb.d"
if [ -d "$INIT_DIR" ]; then
  echo "Executing initialization scripts..."
  for script in "$INIT_DIR"/*; do
    if [ -f "$script" ] && [ -x "$script" ]; then
      echo "Running $script"
      "$script" &
    fi
  done
fi

echo "Starting OpenSearch..."
exec /usr/share/opensearch/bin/opensearch -E plugins.security.disabled=true