#!/bin/bash

echo "=== Kafka Consumer Group Offset Reset Interaktif ==="

# --- Step 1: Pilih bootstrap-server ---
read -p "Masukkan bootstrap-server (contoh: localhost:9092): " BOOTSTRAP_SERVER

# --- Step 2: Pilih container Docker (autocomplete dari container yang running) ---
echo "Daftar container Docker yang sedang running:"
docker ps --format "{{.Names}}"
read -p "Pilih container Docker: " CONTAINER

# --- Step 3: Pilih Kafka topic (ambil daftar dari container) ---
echo "Mengambil daftar topic dari Kafka..."
TOPICS=$(docker exec "$CONTAINER" kafka-topics --bootstrap-server "$BOOTSTRAP_SERVER" --list 2>/dev/null)

if [ -z "$TOPICS" ]; then
    echo "Gagal mengambil topic dari Kafka. Periksa bootstrap-server atau container."
    exit 1
fi

echo "Daftar topic:"
echo "$TOPICS"
read -p "Masukkan nama topic: " TOPIC

# --- Step 4: Pilih consumer group (ambil daftar dari container) ---
echo "Mengambil daftar consumer group untuk topic '$TOPIC'..."
GROUPS=$(docker exec "$CONTAINER" bash -c "kafka-consumer-groups --bootstrap-server $BOOTSTRAP_SERVER --list 2>/dev/null" | awk '{print $1}' | grep -v "^$" | grep -v "GROUP")

if [ -z "$GROUPS" ]; then
    echo "Gagal mengambil consumer group dari Kafka."
    exit 1
fi

echo "Daftar consumer group:"
echo "$GROUPS"
read -p "Masukkan nama consumer group: " GROUP

# --- Step 5: Konfirmasi sebelum eksekusi ---
echo ""
echo "Konfirmasi:"
echo "Bootstrap server: $BOOTSTRAP_SERVER"
echo "Container: $CONTAINER"
echo "Topic: $TOPIC"
echo "Consumer group: $GROUP"
read -p "Apakah mau lanjut mereset offsets? (y/n): " CONFIRM

if [[ "$CONFIRM" != "y" && "$CONFIRM" != "Y" ]]; then
    echo "Batal mereset offsets."
    exit 0
fi

# --- Step 6: Eksekusi reset offsets ---
docker exec -it "$CONTAINER" kafka-consumer-groups \
    --bootstrap-server "$BOOTSTRAP_SERVER" \
    --group "$GROUP" \
    --topic "$TOPIC" \
    --reset-offsets \
    --to-earliest \
    --execute

echo "Reset offsets untuk group '$GROUP' pada topic '$TOPIC' berhasil."
