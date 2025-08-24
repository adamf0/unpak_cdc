#!/bin/bash

echo "=== Kafka Consumer Group Offset Reset Interaktif ==="

# --- Step 1: Pilih bootstrap-server ---
read -p "Masukkan bootstrap-server (contoh: localhost:9092): " BOOTSTRAP_SERVER

# --- Step 2: Pilih container Docker (autocomplete dari container yang running) ---
echo "Daftar container Docker yang sedang running:"
docker ps --format "{{.Names}}"
read -p "Pilih container Docker: " CONTAINER

# --- Step 3: Pilih consumer group (ambil daftar dari container) ---
# echo "Mengambil daftar consumer group untuk topic '$TOPIC'..."
# GROUPS=$(docker exec "$CONTAINER" kafka-consumer-groups --bootstrap-server "$BOOTSTRAP_SERVER" --list 2>/dev/null)

# if [ -z "$GROUPS" ]; then
#     echo "Gagal mengambil consumer group dari Kafka."
#     exit 1
# fi

# echo "Daftar consumer group:"
# echo "$GROUPS"
read -p "Masukkan nama consumer group: " GROUP

# --- Step 4: Konfirmasi sebelum eksekusi ---
echo ""
echo "Konfirmasi:"
echo "Bootstrap server: $BOOTSTRAP_SERVER"
echo "Container: $CONTAINER"
echo "Consumer group: $GROUP"
read -p "Apakah mau lanjut hapus group consumer? (y/n): " CONFIRM

if [[ "$CONFIRM" != "y" && "$CONFIRM" != "Y" ]]; then
    echo "Batal hapus offsets."
    exit 0
fi

# --- Step 6: Eksekusi group consumer ---
docker exec -it "$CONTAINER" kafka-consumer-groups \
    --bootstrap-server "$BOOTSTRAP_SERVER" \
    --group "$GROUP" \
    --delete

echo "Hapus group '$GROUP' berhasil."
