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
)

// Dosen struct sesuai dengan struktur tabel `dosen`
type Dosen struct {
	NIDN              string `json:"NIDN"`
	NipLama           string `json:"nip_lama"`
	NipBaru           string `json:"nip_baru"`
	KodeFak           string `json:"kode_fak"`
	KodeJurusan       string `json:"kode_jurusan"`
	KodeProdi         string `json:"kode_prodi"`
	KodeJenjang       string `json:"kode_jenjang"`
	NamaDosen         string `json:"nama_dosen"`
	StatusAktif       string `json:"status_aktif"`
	PangkatGolongan   string `json:"pangkat_golongan"`
	JabatanAkademik   string `json:"jabatan_akademik"`
	JabatanFungsional string `json:"jabatan_fungsional"`
	JabatanStruktural string `json:"jabatan_struktural"`
	StatusPegawai     string `json:"status_pegawai"`
}

// DebeziumPayload untuk field "payload" dari Kafka message
type DebeziumPayload struct {
	Before *Dosen `json:"before"`
	After  *Dosen `json:"after"`
	Op     string `json:"op"` // c=create, u=update, d=delete, r=read(snapshot)
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
		case "c", "r": // INSERT atau SNAPSHOT/READ
			if payload.After != nil {
				fmt.Printf("(%s) Inserted Dosen: %+v\n", payload.Op, payload.After)
				if err := insertDosen(h.db, h.table, payload.After); err != nil {
					log.Printf("Insert error: %v", err)
				}
			}
		case "u": // UPDATE
			if payload.After != nil {
				fmt.Printf("(%s) Updated Dosen: %+v\n", payload.Op, payload.After)
				if err := updateDosen(h.db, h.table, payload.After); err != nil {
					log.Printf("Update error: %v", err)
				}
			}
		case "d": // DELETE
			if payload.Before != nil {
				fmt.Printf("(%s) Deleted Dosen: %+v\n", payload.Op, payload.Before)
				if err := deleteDosen(h.db, h.table, payload.Before.NIDN); err != nil {
					log.Printf("Delete error: %v", err)
				}
			}
		}

		session.MarkMessage(msg, "")
	}
	return nil
}

// Insert Dosen
func insertDosen(db *sql.DB, tbl string, d *Dosen) error {
	query := fmt.Sprintf(`
		INSERT INTO %s 
		(NIDN, nip_lama, nip_baru, kode_fak, kode_jurusan, kode_prodi, kode_jenjang, nama_dosen, status_aktif, pangkat_golongan, jabatan_akademik, jabatan_fungsional, jabatan_struktural, status_pegawai)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
			nip_lama = VALUES(nip_lama),
			nip_baru = VALUES(nip_baru),
			kode_fak = VALUES(kode_fak),
			kode_jurusan = VALUES(kode_jurusan),
			kode_prodi = VALUES(kode_prodi),
			kode_jenjang = VALUES(kode_jenjang),
			nama_dosen = VALUES(nama_dosen),
			status_aktif = VALUES(status_aktif),
			pangkat_golongan = VALUES(pangkat_golongan),
			jabatan_akademik = VALUES(jabatan_akademik),
			jabatan_fungsional = VALUES(jabatan_fungsional),
			jabatan_struktural = VALUES(jabatan_struktural),
			status_pegawai = VALUES(status_pegawai)
	`, tbl)

	_, err := db.Exec(query,
		d.NIDN, d.NipLama, d.NipBaru, d.KodeFak, d.KodeJurusan, d.KodeProdi, d.KodeJenjang,
		d.NamaDosen, d.StatusAktif, d.PangkatGolongan, d.JabatanAkademik, d.JabatanFungsional,
		d.JabatanStruktural, d.StatusPegawai)
	return err
}

// Update Dosen
func updateDosen(db *sql.DB, tbl string, d *Dosen) error {
	query := fmt.Sprintf(`
		UPDATE %s SET 
			nip_lama=?, nip_baru=?, kode_fak=?, kode_jurusan=?, kode_prodi=?, kode_jenjang=?, 
			nama_dosen=?, status_aktif=?, pangkat_golongan=?, jabatan_akademik=?, 
			jabatan_fungsional=?, jabatan_struktural=?, status_pegawai=?
		WHERE NIDN=?
	`, tbl)

	_, err := db.Exec(query,
		d.NipLama, d.NipBaru, d.KodeFak, d.KodeJurusan, d.KodeProdi, d.KodeJenjang,
		d.NamaDosen, d.StatusAktif, d.PangkatGolongan, d.JabatanAkademik, d.JabatanFungsional,
		d.JabatanStruktural, d.StatusPegawai, d.NIDN)
	return err
}

// Delete Dosen
func deleteDosen(db *sql.DB, tbl string, NIDN string) error {
	query := fmt.Sprintf(`DELETE FROM %s WHERE NIDN=?`, tbl)
	_, err := db.Exec(query, NIDN)
	return err
}
