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
	"sync"
	"syscall"
	"time"

	"github.com/IBM/sarama"
	"github.com/tidwall/gjson"
	_ "github.com/go-sql-driver/mysql"
)

var (
	dbSQL *sql.DB

	// cache fakultas & prodi
	fakultasCache = make(map[string]string)
	prodiCache    = make(map[string]string)
	cacheMu       sync.RWMutex
)

const (
	defaultBrokers    = "localhost:9092"
	defaultTopicFak   = "simak2.unpak_simak.m_fakultas"
	defaultTopicProdi = "simak4.unpak_simak.m_program_studi"
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
	topicFak := mustEnv("TOPIC_FAKULTAS", defaultTopicFak)
	topicProdi := mustEnv("TOPIC_PRODI", defaultTopicProdi)
	topicDosen := mustEnv("TOPIC_DOSEN", defaultTopicDosen)

	log.Printf("▶️  starting joiner-service")
	log.Printf("    brokers    : %s", brokers)
	log.Printf("    topicFak   : %s", topicFak)
	log.Printf("    topicProdi : %s", topicProdi)
	log.Printf("    topicDosen : %s", topicDosen)

	initMariaDB()
	defer dbSQL.Close()

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
		topicFak:   topicFak,
		topicProdi: topicProdi,
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
			if err := consumer.Consume(ctx, []string{topicFak, topicProdi, topicDosen}, handler); err != nil {
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
	topicFak   string
	topicProdi string
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
	case h.topicFak:
		h.handleFakultas(before, after, op)
	case h.topicProdi:
		h.handleProdi(before, after, op)
	case h.topicDosen:
		h.handleDosen(before, after, op)
	}
}

// ---------------- Handlers ----------------
func (h *consumerHandler) handleFakultas(before, after gjson.Result, op *string) {
	if after.Exists() {
		kode := after.Get("kode_fakultas").String()
		nama := after.Get("nama_fakultas").String()
		cacheMu.Lock()
		fakultasCache[kode] = nama
		cacheMu.Unlock()
		*op = "FAKULTAS_UPSERT"
	} else if before.Exists() {
		kode := before.Get("kode_fakultas").String()
		cacheMu.Lock()
		delete(fakultasCache, kode)
		cacheMu.Unlock()
		*op = "FAKULTAS_DELETE"
	}
}

func (h *consumerHandler) handleProdi(before, after gjson.Result, op *string) {
	if after.Exists() {
		kode := after.Get("kode_prodi").String()
		nama := after.Get("nama_prodi").String()
		cacheMu.Lock()
		prodiCache[kode] = nama
		cacheMu.Unlock()
		*op = "PRODI_UPSERT"
	} else if before.Exists() {
		kode := before.Get("kode_prodi").String()
		cacheMu.Lock()
		delete(prodiCache, kode)
		cacheMu.Unlock()
		*op = "PRODI_DELETE"
	}
}

func (h *consumerHandler) handleDosen(before, after gjson.Result, op *string) {
	if after.Exists() {
		*op = "DOSEN_UPSERT"

		cacheMu.RLock()
		namaFak := fakultasCache[after.Get("kode_fak").String()]
		namaProdi := prodiCache[after.Get("kode_prodi").String()]
		cacheMu.RUnlock()

		d := DosenJoined{
			NIDN:         after.Get("NIDN").String(),
			NIPLama:      after.Get("nip_lama").String(),
			NIPBaru:      after.Get("nip_baru").String(),
			KodeJurusan:  after.Get("kode_jurusan").String(),
			KodeJenjang:  after.Get("kode_jenjang").String(),
			NamaDosen:    after.Get("nama_dosen").String(),
			KodeFak:      after.Get("kode_fak").String(),
			NamaFakultas: namaFak,
			KodeProdi:    after.Get("kode_prodi").String(),
			NamaProdi:    namaProdi,
		}

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

		if b, err := json.MarshalIndent(d, "", "  "); err == nil {
			fmt.Println("Upsert:\n" + string(b))
		} else{
			log.Printf("❌ Upsert: %v", err)
		}
		// if _, err := dbSQL.Exec(q,
		// 	d.NIDN, d.NIPLama, d.NIPBaru, d.KodeJurusan, d.KodeJenjang, d.NamaDosen,
		// 	d.KodeFak, d.NamaFakultas, d.KodeProdi, d.NamaProdi,
		// ); err != nil {
		// 	log.Printf("❌ upsert dosen: %v", err)
		// } else if b, err := json.MarshalIndent(d, "", "  "); err == nil {
		// 	fmt.Println("Upsert:\n" + string(b))
		// }
	} else if before.Exists() {
		*op = "DOSEN_DELETE"
		nidn := before.Get("NIDN").String()
		// q := `DELETE FROM dosen_joined WHERE nidn=?`

		fmt.Println("Delete:\n" + nidn)
		// if _, err := dbSQL.Exec(q, nidn); err != nil {
		// 	log.Printf("❌ delete dosen: %v", err)
		// } else {
		// 	fmt.Println("Delete:", nidn)
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
