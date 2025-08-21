package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	"github.com/IBM/sarama"
)

// Pengguna struct sesuai dengan struktur tabel `pengguna`
type Pengguna struct {
	Id int `json:"id"`
	Username string `json:"username"`
	Password string `json:"password"`
	Level string `json:"level"`
	Status string `json:"status"`
}

// DebeziumPayload untuk field "payload" dari Kafka message
type DebeziumPayload struct {
	Before *Pengguna `json:"before"`
	After  *Pengguna `json:"after"`
	Op     string    `json:"op"` // c=create, u=update, d=delete, r=read(snapshot)
}

// KafkaMessage mencakup payload di dalam JSON root
type KafkaMessage struct {
	Payload DebeziumPayload `json:"payload"`
}

func main() {
	// Koneksi database
	// dsn := "cdc:unp@kcdc0k3@tcp(172.16.20.245:3306)/unpak_simonev?parseTime=true&loc=Local"
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

	// Kafka config
	// brokers := []string{"localhost:9092"}
	brokersEnv := os.Getenv("BROKERS")
	if brokersEnv == "" {
		log.Fatal("BROKERS environment variable not set")
	}
	brokers := strings.Split(brokersEnv, ",")

	// groupID := "pengguna-consumeto-simonev"
	groupID := os.Getenv("GROUP_ID")
	if groupID == "" {
		log.Fatal("GROUP_ID environment variable not set")
	}

	// topic := "simpeg2.unpak_simpeg.pengguna"
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

	handler := ConsumerGroupHandler{topic: topic, db: db, table: tbl}

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
}

func (ConsumerGroupHandler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (ConsumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (h ConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		var km KafkaMessage
		if err := json.Unmarshal(msg.Value, &km); err != nil {
			log.Printf("Error unmarshalling message: %v", err)
			continue
		}

		// tbl := "pengguna_simpeg";
		payload := km.Payload
		switch payload.Op {
		case "c", "r": // INSERT atau SNAPSHOT/READ
			if payload.After != nil {
				fmt.Printf("(%s) Inserted pengguna: %+v\n", payload.Op, payload.After)
				if err := insertPengguna(h.db, h.table, payload.After); err != nil {
					log.Printf("Insert error: %v", err)
				}
			}
		case "u": // UPDATE
			if payload.After != nil {
				fmt.Printf("(%s) Updated pengguna: %+v\n", payload.Op, payload.After)
				if err := updatePengguna(h.db, h.table, payload.After); err != nil {
					log.Printf("Update error: %v", err)
				}
			}
		case "d": // DELETE
			if payload.Before != nil {
				fmt.Printf("(%s) Deleted pengguna: %+v\n", payload.Op, payload.Before)
				if err := deletePengguna(h.db, h.table, payload.Before.Id); err != nil {
					log.Printf("Delete error: %v", err)
				}
			}
		}

		session.MarkMessage(msg, "")
	}
	return nil
}

// Insert pengguna
func insertPengguna(db *sql.DB, tbl string, f *Pengguna) error {
	query := fmt.Sprintf(`
		INSERT INTO %s (id, username, password, level, status)
		VALUES (?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
			username=VALUES(username),
			password=VALUES(password),
			level=VALUES(level),
			status=VALUES(status)
	`, tbl)

	_, err := db.Exec(query, u.Id, u.Username, u.Password, u.Level, u.Status)
	return err
}

// Update pengguna
func updatePengguna(db *sql.DB, tbl string, f *Pengguna) error {
	query := fmt.Sprintf(`
		UPDATE %s SET
			username=?,
			password=?,
			level=?,
			status=?
		WHERE id=?
	`, tbl)

	_, err := db.Exec(query, u.Username, u.Password, u.Level, u.Status, u.Id)
	return err
}

// Delete pengguna
func deletePengguna(db *sql.DB, tbl string, id int) error {
	query := fmt.Sprintf(`DELETE FROM %s WHERE id=?`, tbl)
	_, err := db.Exec(query, id)
	return err
}
