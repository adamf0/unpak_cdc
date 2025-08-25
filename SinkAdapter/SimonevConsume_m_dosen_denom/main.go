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

	"github.com/IBM/sarama"
	"github.com/redis/go-redis/v9"
	"github.com/tidwall/gjson"
	_ "github.com/go-sql-driver/mysql"
)

var (
	dbSQL  *sql.DB
	rdb    *redis.Client
	ctxBg  = context.Background()
)

const (
	defaultBrokers    = "localhost:9092"
	defaultTopicDosen = "simak5.unpak_simak.m_dosen"
)

type DosenJoined struct {
	NIDN         string `json:"nidn"`
	NIPLama      string `json:"nip_lama"`
	NIPBaru      string `json:"nip_baru"`
	KodeJurusan  string `json:"kode_jurusan"`
	KodeJenjang  string `json:"kode_jenjang"`
	NamaDosen    string `json:"nama_dosen"`
	KodeFak      string `json:"kode_fak"`
	NamaFakultas string `json:"nama_fakultas"`
	KodeProdi    string `json:"kode_prodi"`
	NamaProdi    string `json:"nama_prodi"`
}

func mustEnv(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

func main() {
	brokers := mustEnv("KAFKA_BROKERS", defaultBrokers)
	topicDosen := mustEnv("TOPIC_DOSEN", defaultTopicDosen)

	log.Printf("▶️  starting joiner-service")
	log.Printf("    brokers    : %s", brokers)
	log.Printf("    topicDosen : %s", topicDosen)

	initMariaDB()
	defer dbSQL.Close()

	initRedis()
	defer rdb.Close()

	cfg := sarama.NewConfig()
	cfg.Version = sarama.V2_8_0_0
	cfg.Consumer.Return.Errors = true
	cfg.Consumer.Offsets.Initial = sarama.OffsetOldest

	groupID := mustEnv("CONSUMER_GROUP", "joiner-group")
	consumer, err := sarama.NewConsumerGroup(strings.Split(brokers, ","), groupID, cfg)
	if err != nil {
		log.Fatalf("❌ create consumer group: %v", err)
	}
	defer consumer.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	handler := &consumerHandler{
		topicDosen: topicDosen,
	}

	// error listener
	go func() {
		for err := range consumer.Errors() {
			log.Printf("⚠️  consumer error: %v", err)
		}
	}()

	// consuming loop
	go func() {
		for {
			if err := consumer.Consume(ctx, []string{topicDosen}, handler); err != nil {
				log.Printf("⚠️  consume error: %v", err)
				time.Sleep(time.Second)
			}
		}
	}()

	// graceful shutdown
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	log.Println("🛑 shutting down joiner...")
	cancel()
}

// ---------------- Consumer ----------------
type consumerHandler struct {
	topicDosen string
}

func (h *consumerHandler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (h *consumerHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (h *consumerHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		op := "UNKNOWN"
		begin := time.Now()
		h.process(msg.Topic, msg.Value, &op)
		log.Printf("✅ topic=%s op=%s offset=%d took=%s", msg.Topic, op, msg.Offset, time.Since(begin).Truncate(time.Millisecond))
		sess.MarkMessage(msg, "")
	}
	return nil
}

func (h *consumerHandler) process(topic string, value []byte, op *string) {
	payload := gjson.GetBytes(value, "payload")
	var before, after gjson.Result
	if payload.Exists() {
		before = payload.Get("before")
		after = payload.Get("after")
	} else {
		before = gjson.GetBytes(value, "before")
		after = gjson.GetBytes(value, "after")
	}

	if !after.Exists() && !before.Exists() {
		*op = "SKIP"
		return
	}

	switch topic {
	case h.topicDosen:
		h.handleDosen(before, after, op)
	}
}

// ---------------- Handlers ----------------
func (h *consumerHandler) handleDosen(before, after gjson.Result, op *string) {
	if after.Exists() {
		*op = "DOSEN_UPSERT"

		kodeFak := after.Get("kode_fak").String()
		kodeProdi := after.Get("kode_prodi").String()

		// ambil nama fakultas & prodi dari redis
		namaFak, _ := rdb.Get(ctxBg, "fakultas#"+kodeFak).Result()
		namaProdi, _ := rdb.Get(ctxBg, "prodi#"+kodeProdi).Result()

		d := DosenJoined{
			NIDN:         after.Get("NIDN").String(),
			NIPLama:      after.Get("nip_lama").String(),
			NIPBaru:      after.Get("nip_baru").String(),
			KodeJurusan:  after.Get("kode_jurusan").String(),
			KodeJenjang:  after.Get("kode_jenjang").String(),
			NamaDosen:    after.Get("nama_dosen").String(),
			KodeFak:      kodeFak,
			NamaFakultas: namaFak,
			KodeProdi:    kodeProdi,
			NamaProdi:    namaProdi,
		}

		if b, err := json.MarshalIndent(d, "", "  "); err == nil {
			fmt.Println("Upsert:\n" + string(b))
		} else {
			log.Printf("❌ Upsert: %v", err)
		}

		// contoh jika mau insert ke MariaDB
		// q := `INSERT INTO dosen_joined 
		//       (nidn, nip_lama, nip_baru, kode_jurusan, kode_jenjang, nama_dosen, kode_fak, nama_fakultas, kode_prodi, nama_prodi)
		//       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		//       ON DUPLICATE KEY UPDATE
		//         nip_lama=VALUES(nip_lama),
		//         nip_baru=VALUES(nip_baru),
		//         kode_jurusan=VALUES(kode_jurusan),
		//         kode_jenjang=VALUES(kode_jenjang),
		//         nama_dosen=VALUES(nama_dosen),
		//         kode_fak=VALUES(kode_fak),
		//         nama_fakultas=VALUES(nama_fakultas),
		//         kode_prodi=VALUES(kode_prodi),
		//         nama_prodi=VALUES(nama_prodi)`
		// if _, err := dbSQL.Exec(q,
		// 	d.NIDN, d.NIPLama, d.NIPBaru, d.KodeJurusan, d.KodeJenjang, d.NamaDosen,
		// 	d.KodeFak, d.NamaFakultas, d.KodeProdi, d.NamaProdi,
		// ); err != nil {
		// 	log.Printf("❌ upsert dosen: %v", err)
		// }
	} else if before.Exists() {
		*op = "DOSEN_DELETE"
		nidn := before.Get("NIDN").String()
		fmt.Println("Delete:\n" + nidn)

		// q := `DELETE FROM dosen_joined WHERE nidn=?`
		// if _, err := dbSQL.Exec(q, nidn); err != nil {
		// 	log.Printf("❌ delete dosen: %v", err)
		// }
	}
}

// ---------------- MariaDB ----------------
func initMariaDB() {
	dsn := mustEnv("MYSQL_DSN", "cdc:unp@kcdc0k3@tcp(172.16.20.245:3306)/unpak_simonev?parseTime=true")
	var err error
	dbSQL, err = sql.Open("mysql", dsn)
	if err != nil {
		log.Fatalf("❌ mysql connect: %v", err)
	}
	if err = dbSQL.Ping(); err != nil {
		log.Fatalf("❌ mysql ping: %v", err)
	}
	log.Println("✅ connected to MariaDB")
}

// ---------------- Redis ----------------
func initRedis() {
	addr := mustEnv("REDIS_ADDR", "localhost:6379")
	pass := mustEnv("REDIS_PASS", "")

	rdb = redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: pass,
		DB:       0,
	})

	if err := rdb.Ping(ctxBg).Err(); err != nil {
		log.Fatalf("❌ redis connect: %v", err)
	}
	log.Println("✅ connected to Redis")
}
