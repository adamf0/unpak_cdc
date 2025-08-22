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
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/IBM/sarama"
)

// Custom type untuk Debezium date (int32 days since 1970-01-01)
type DebeziumDate struct {
	Time  *time.Time
	Valid bool
}

func (d *DebeziumDate) UnmarshalJSON(b []byte) error {
	s := strings.TrimSpace(string(b))
	if s == "null" || s == "" {
		d.Time = nil
		d.Valid = false
		return nil
	}

	var days int32
	if err := json.Unmarshal(b, &days); err != nil {
		return err
	}

	t := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, int(days))
	d.Time = &t
	d.Valid = true
	return nil
}

// PayrollMPegawai struct sesuai tabel payroll_m_pegawai
type PayrollMPegawai struct {
	Id         int           `json:"id_pegawai"`
	NoMesin    string        `json:"no_mesin"`
	NIP        string        `json:"nip"`
	Nama       string        `json:"nama"`
	JK         string        `json:"jk"`
	Fakultas   string        `json:"fakultas"`
	Prodi      string        `json:"prodi"`
	Struktural string        `json:"struktural"`
	Fungsional string        `json:"fungsional"`
	Golongan   string        `json:"golongan"`
	Status     string        `json:"status"`
	TglMasuk   DebeziumDate  `json:"tgl_masuk"`
	TglKeluar  *string       `json:"tgl_keluar"`
}

// DebeziumPayload untuk Kafka payload
type DebeziumPayload struct {
	Before *PayrollMPegawai `json:"before"`
	After  *PayrollMPegawai `json:"after"`
	Op     string           `json:"op"` // c=create, u=update, d=delete, r=read(snapshot)
}

// KafkaMessage root
type KafkaMessage struct {
	Payload DebeziumPayload `json:"payload"`
}

func main() {
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

		payload := km.Payload

		switch payload.Op {
		case "c", "r":
			if payload.After != nil {
				fmt.Printf("(%s) Inserted PayrollMPegawai: %+v\n", payload.Op, payload.After)
				if err := insertPayrollMPegawai(h.db, h.table, payload.After); err != nil {
					log.Printf("Insert error: %v", err)
				}
			}
		case "u":
			if payload.After != nil {
				fmt.Printf("(%s) Updated PayrollMPegawai: %+v\n", payload.Op, payload.After)
				if err := updatePayrollMPegawai(h.db, h.table, payload.After); err != nil {
					log.Printf("Update error: %v", err)
				}
			}
		case "d":
			if payload.Before != nil {
				fmt.Printf("(%s) Deleted PayrollMPegawai: %+v\n", payload.Op, payload.Before)
				if err := deletePayrollMPegawai(h.db, h.table, payload.Before.Id); err != nil {
					log.Printf("Delete error: %v", err)
				}
			}
		}

		session.MarkMessage(msg, "")
	}
	return nil
}

// INSERT dengan upsert
func insertPayrollMPegawai(db *sql.DB, tbl string, f *PayrollMPegawai) error {
	query := fmt.Sprintf(`
		INSERT INTO %s
		(id_pegawai, no_mesin, nip, nama, jk, fakultas, prodi, struktural, fungsional, golongan, status, tgl_masuk, tgl_keluar)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
			no_mesin=VALUES(no_mesin),
			nip=VALUES(nip),
			nama=VALUES(nama),
			jk=VALUES(jk),
			fakultas=VALUES(fakultas),
			prodi=VALUES(prodi),
			struktural=VALUES(struktural),
			fungsional=VALUES(fungsional),
			golongan=VALUES(golongan),
			status=VALUES(status),
			tgl_masuk=VALUES(tgl_masuk),
			tgl_keluar=VALUES(tgl_keluar)
	`, tbl)

	var tglMasuk interface{}
	if f.TglMasuk.Valid {
		tglMasuk = f.TglMasuk.Time
	}

	_, err := db.Exec(query,
		f.Id, f.NoMesin, f.NIP, f.Nama, f.JK, f.Fakultas, f.Prodi,
		f.Struktural, f.Fungsional, f.Golongan, f.Status,
		tglMasuk, f.TglKeluar,
	)
	return err
}

// UPDATE
func updatePayrollMPegawai(db *sql.DB, tbl string, f *PayrollMPegawai) error {
	query := fmt.Sprintf(`
		UPDATE %s SET
			no_mesin=?,
			nip=?,
			nama=?,
			jk=?,
			fakultas=?,
			prodi=?,
			struktural=?,
			fungsional=?,
			golongan=?,
			status=?,
			tgl_masuk=?,
			tgl_keluar=?
		WHERE id_pegawai=?
	`, tbl)

	var tglMasuk interface{}
	if f.TglMasuk.Valid {
		tglMasuk = f.TglMasuk.Time
	}

	_, err := db.Exec(query,
		f.NoMesin, f.NIP, f.Nama, f.JK, f.Fakultas, f.Prodi,
		f.Struktural, f.Fungsional, f.Golongan, f.Status,
		tglMasuk, f.TglKeluar,
		f.Id,
	)
	return err
}

// DELETE
func deletePayrollMPegawai(db *sql.DB, tbl string, id int) error {
	query := fmt.Sprintf(`DELETE FROM %s WHERE id_pegawai=?`, tbl)
	_, err := db.Exec(query, id)
	return err
}
