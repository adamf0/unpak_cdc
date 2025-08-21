#!/bin/bash
set -e

CONNECT_URL="https://cdc-api.unpak.ac.id/connectors"

# ambil semua file json dari folder connectors di dalam folder yang sama
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

for file in "$SCRIPT_DIR"/connectors/*.json; do
  name=$(jq -r .name "$file")
  echo ">> Setting up connector: $name"

  # cek apakah sudah ada
  if curl -s "$CONNECT_URL/$name" | grep '"name"' > /dev/null; then
    echo "Connector $name already exists, skipping..."
  else
    curl -X POST -H "Content-Type: application/json" --data @"$file" "$CONNECT_URL"
  fi
done

echo ">> All connectors processed."
