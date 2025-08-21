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

// User struct sesuai dengan struktur tabel `user`
type User struct {
	UserID       string `json:"userid"`
	Username     string `json:"username"`
	Password     string `json:"password"`
	Nama         string `json:"nama"`
	Email        string `json:"email"`
	Level        string `json:"level"`
	Aktif        string `json:"aktif"`
}

// DebeziumPayload untuk field "payload" dari Kafka message
type DebeziumPayload struct {
	Before *User  `json:"before"`
	After  *User  `json:"after"`
	Op     string `json:"op"` // c=create, u=update, d=delete, r=read(snapshot)
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

	// groupID := "user-consumeto-simonev"
	groupID := os.Getenv("GROUP_ID")
	if groupID == "" {
		log.Fatal("GROUP_ID environment variable not set")
	}

	// topic := "simak1.unpak_simak.user"
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

		// tbl := "user_simak"
		payload := km.Payload
		switch payload.Op {
		case "c", "r": // INSERT atau SNAPSHOT/READ
			userJSON, _ := json.Marshal(payload.After)
			fmt.Printf("(%s) Inserted user: %s\n", payload.Op, string(userJSON))

			if payload.After != nil {
				if err := insertUser(h.db, h.table, payload.After); err != nil {
					log.Printf("Insert error: %v", err)
				} else {
					fmt.Printf("Inserted user: %s\n", payload.After.UserID)
				}
			}
		case "u": // UPDATE
			userJSON, _ := json.Marshal(payload.After)
			fmt.Printf("(%s) Updated user: %s\n", payload.Op, string(userJSON))

			if payload.After != nil {
				if err := updateUser(h.db, h.table, payload.After); err != nil {
					log.Printf("Update error: %v", err)
				} else {
					fmt.Printf("Updated user: %s\n", payload.After.UserID)
				}
			}
		case "d": // DELETE
			userJSON, _ := json.Marshal(payload.After)
			fmt.Printf("(%s) Updated user: %s\n", payload.Op, string(userJSON))

			if payload.Before != nil {
				if err := deleteUser(h.db, h.table, payload.Before.UserID); err != nil {
					log.Printf("Delete error: %v", err)
				} else {
					fmt.Printf("Deleted user: %s\n", payload.Before.UserID)
				}
			}
		}

		session.MarkMessage(msg, "")
	}
	return nil
}

// Insert user
func insertUser(db *sql.DB, tbl string, u *User) error {
	query := fmt.Sprintf(`
		INSERT INTO %s (userid, username, password, nama, email, level, aktif)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
			username=VALUES(username),
			password=VALUES(password),
			nama=VALUES(nama),
			email=VALUES(email),
			level=VALUES(level),
			aktif=VALUES(aktif)
	`, tbl)

	_, err := db.Exec(query, u.UserID, u.Username, u.Password, u.Nama, u.Email, u.Level, u.Aktif)
	return err
}

// Update user
func updateUser(db *sql.DB, tbl string, u *User) error {
	query := fmt.Sprintf(`
		UPDATE %s SET
			username=?, 
			password=?, 
			nama=?, 
			email=?, 
			level=?, 
			aktif=? 
		WHERE userid=?
	`, tbl)

	_, err := db.Exec(query, u.Username, u.Password, u.Nama, u.Email, u.Level, u.Aktif, u.UserID)
	return err
}

// Delete user
func deleteUser(db *sql.DB, tbl string, userID string) error {
	query := fmt.Sprintf(`DELETE FROM %s WHERE userid=?`, tbl)
	_, err := db.Exec(query, userID)
	return err
}
