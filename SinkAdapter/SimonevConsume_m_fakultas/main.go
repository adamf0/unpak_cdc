package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	_ "github.com/go-sql-driver/mysql"
	"github.com/IBM/sarama"
	"github.com/redis/go-redis/v9"
)

// Fakultas struct sesuai dengan struktur tabel `fakultas`
type Fakultas struct {
	KodeFakultas string `json:"kode_fakultas"`
	NamaFakultas string `json:"nama_fakultas"`
}

// DebeziumPayload untuk field "payload" dari Kafka message
type DebeziumPayload struct {
	Before *Fakultas `json:"before"`
	After  *Fakultas `json:"after"`
	Op     string    `json:"op"` // c=create, u=update, d=delete, r=read(snapshot)
}

// KafkaMessage mencakup payload di dalam JSON root
type KafkaMessage struct {
	Payload DebeziumPayload `json:"payload"`
}

func main() {
	// Koneksi database
	dsn := os.Getenv("DSN")
	if dsn == "" {
		log.Fatal("DSN environment variable not set")
	}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		log.Fatalf("Database ping failed: %v", err)
	}
	fmt.Println("Connected to MariaDB!")

	// Koneksi Redis
	rdb := redis.NewClient(&redis.Options{
		Addr:     os.Getenv("REDIS_ADDR"), // contoh: localhost:6379
		Password: os.Getenv("REDIS_PASS"), // "" kalau tanpa password
		DB:       0,
	})

	if err := rdb.Ping(context.Background()).Err(); err != nil {
		log.Fatalf("Failed to connect to Redis: %v", err)
	}
	fmt.Println("Connected to Redis!")

	// Kafka config
	brokersEnv := os.Getenv("BROKERS")
	if brokersEnv == "" {
		log.Fatal("BROKERS environment variable not set")
	}
	brokers := strings.Split(brokersEnv, ",")

	groupID := os.Getenv("GROUP_ID")
	if groupID == "" {
		log.Fatal("GROUP_ID environment variable not set")
	}

	topic := os.Getenv("TOPIC")
	if topic == "" {
		log.Fatal("TOPIC environment variable not set")
	}

	tbl := os.Getenv("TABLE")
	if tbl == "" {
		log.Fatal("TABLE environment variable not set")
	}

	config := sarama.NewConfig()
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	config.Version = sarama.V3_4_0_0
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	consumerGroup, err := sarama.NewConsumerGroup(brokers, groupID, config)
	if err != nil {
		log.Fatalf("Error creating consumer group: %v", err)
	}
	defer consumerGroup.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		sigterm := make(chan os.Signal, 1)
		signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
		<-sigterm
		cancel()
	}()

	handler := ConsumerGroupHandler{
		topic: topic,
		db:    db,
		table: tbl,
		rdb:   rdb,
	}

	for {
		if err := consumerGroup.Consume(ctx, []string{topic}, handler); err != nil {
			log.Fatalf("Error consuming: %v", err)
		}
		if ctx.Err() != nil {
			return
		}
	}
}

type ConsumerGroupHandler struct {
	topic string
	db    *sql.DB
	table string
	rdb   *redis.Client
}

func (ConsumerGroupHandler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (ConsumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (h ConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	ctx := context.Background()

	for msg := range claim.Messages() {
		var km KafkaMessage
		if err := json.Unmarshal(msg.Value, &km); err != nil {
			log.Printf("Error unmarshalling message: %v", err)
			continue
		}

		payload := km.Payload
		switch payload.Op {
		case "c", "r": // INSERT / SNAPSHOT
			if payload.After != nil {
				fmt.Printf("(%s) Inserted fakultas: %+v\n", payload.Op, payload.After)
				if err := insertFakultas(h.db, h.table, payload.After); err != nil {
					log.Printf("Insert error: %v", err)
				}
				if err := saveToRedis(ctx, h.rdb, payload.After); err != nil {
					log.Printf("Redis insert error: %v", err)
				}
			}
		case "u": // UPDATE
			if payload.After != nil {
				fmt.Printf("(%s) Updated fakultas: %+v\n", payload.Op, payload.After)
				if err := updateFakultas(h.db, h.table, payload.After); err != nil {
					log.Printf("Update error: %v", err)
				}
				if err := saveToRedis(ctx, h.rdb, payload.After); err != nil {
					log.Printf("Redis update error: %v", err)
				}
			}
		case "d": // DELETE
			if payload.Before != nil {
				fmt.Printf("(%s) Deleted fakultas: %+v\n", payload.Op, payload.Before)
				if err := deleteFakultas(h.db, h.table, payload.Before.KodeFakultas); err != nil {
					log.Printf("Delete error: %v", err)
				}
				if err := deleteFromRedis(ctx, h.rdb, payload.Before.KodeFakultas); err != nil {
					log.Printf("Redis delete error: %v", err)
				}
			}
		}

		session.MarkMessage(msg, "")
	}
	return nil
}

// Insert fakultas
func insertFakultas(db *sql.DB, tbl string, f *Fakultas) error {
	query := fmt.Sprintf(`
		INSERT INTO %s (kode_fakultas, nama_fakultas)
		VALUES (?, ?)
		ON DUPLICATE KEY UPDATE
			nama_fakultas=VALUES(nama_fakultas)
	`, tbl)

	_, err := db.Exec(query, f.KodeFakultas, f.NamaFakultas)
	return err
}

// Update fakultas
func updateFakultas(db *sql.DB, tbl string, f *Fakultas) error {
	query := fmt.Sprintf(`
		UPDATE %s SET
			nama_fakultas=?
		WHERE kode_fakultas=?
	`, tbl)

	_, err := db.Exec(query, f.NamaFakultas, f.KodeFakultas)
	return err
}

// Delete fakultas
func deleteFakultas(db *sql.DB, tbl string, kodeFakultas string) error {
	query := fmt.Sprintf(`DELETE FROM %s WHERE kode_fakultas=?`, tbl)
	_, err := db.Exec(query, kodeFakultas)
	return err
}

// Save / update Fakultas ke Redis (dengan cek exist)
func saveToRedis(ctx context.Context, rdb *redis.Client, f *Fakultas) error {
	key := "fakultas#" + f.KodeFakultas

	exists, err := rdb.Exists(ctx, key).Result()
	if err != nil {
		return err
	}

	data, err := json.Marshal(f)
	if err != nil {
		return err
	}

	if exists > 0 {
		fmt.Printf("Redis update key=%s\n", key)
		return rdb.Set(ctx, key, data, 0).Err()
	} else {
		fmt.Printf("Redis insert key=%s\n", key)
		return rdb.Set(ctx, key, data, 0).Err()
	}
}

// Delete Fakultas dari Redis (dengan cek exist)
func deleteFromRedis(ctx context.Context, rdb *redis.Client, kode string) error {
	key := "fakultas#" + kode

	exists, err := rdb.Exists(ctx, key).Result()
	if err != nil {
		return err
	}

	if exists > 0 {
		fmt.Printf("Redis delete key=%s\n", key)
		return rdb.Del(ctx, key).Err()
	}

	fmt.Printf("Redis key=%s not found, skip delete\n", key)
	return nil
}
