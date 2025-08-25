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
	defaultTopicMahasiswa = "simak6.unpak_simak.m_mahasiswa"
)

type MahasiswaJoined struct {
	NIM           string `json:"NIM"`
	KodeFak      string `json:"kode_fak"`
	NamaFakultas string `json:"nama_fakultas"`
	NamaFakultasMap1 string `json:"nama_fakultas_map1"`
	NamaFakultasMap2 string `json:"nama_fakultas_map2"`
	KodeJurusan   string `json:"kode_jurusan"`
	KodeJenjang   string `json:"kode_jenjang"`
	JenjangMap1  string `json:"jenjang_map1"`
	JenjangMap2  string `json:"jenjang_map2"`
	KodeProdi    string `json:"kode_prodi"`
	NamaProdi    string `json:"nama_prodi"`
	NamaProdiMap1    string `json:"nama_prodi_map1"`
	NamaProdiMap2    string `json:"nama_prodi_map2"`
	NamaMahasiswa string `json:"nama_mahasiswa"`
	StatusAktif   string `json:"status_aktif"`
	TahunMasuk    string `json:"tahun_masuk"`
	ProdiFakultas1   string `json:"prodi_fakultas1"`
	ProdiFakultas2   string `json:"prodi_fakultas2"`
}

func mustEnv(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

func main() {
	brokers := mustEnv("BROKERS", defaultBrokers)
	topicMahasiswa := mustEnv("TOPIC", defaultTopicMahasiswa)

	log.Printf("▶️  starting joiner-service")
	log.Printf("    brokers    : %s", brokers)
	log.Printf("    topicMahasiswa : %s", topicMahasiswa)

	initMariaDB()
	defer dbSQL.Close()

	initRedis()
	defer rdb.Close()

	cfg := sarama.NewConfig()
	cfg.Version = sarama.V2_8_0_0
	cfg.Consumer.Return.Errors = true
	cfg.Consumer.Offsets.Initial = sarama.OffsetOldest

	groupID := mustEnv("GROUP_ID", "mahasiswa_denom")
	consumer, err := sarama.NewConsumerGroup(strings.Split(brokers, ","), groupID, cfg)
	if err != nil {
		log.Fatalf("❌ create consumer group: %v", err)
	}
	defer consumer.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tbl := os.Getenv("TABLE")
	if tbl == "" {
		log.Fatal("TABLE environment variable not set")
	}

	handler := &consumerHandler{
		topicMahasiswa: topicMahasiswa,
		tbl: tbl,
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
			if err := consumer.Consume(ctx, []string{topicMahasiswa}, handler); err != nil {
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
	topicMahasiswa string
	tbl string
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
	case h.topicMahasiswa:
		h.handleMahasiswa(before, after, op)
	}
}

func mapJenjangV1(kode string) string {
	switch kode {
	case "C":
		return "S1"
	case "B":
		return "S2"
	case "A":
		return "S3"
	case "E":
		return "D3"
	case "D":
		return "D4"
	case "J":
		return "Profesi"
	default:
		return "?"
	}
}
func mapJenjangV2(kode string) string {
	switch kode {
	case "C":
		return "(S1)"
	case "B":
		return "(S2)"
	case "A":
		return "(S3)"
	case "E":
		return "(D3)"
	case "D":
		return "(D4)"
	case "J":
		return "(Profesi)"
	default:
		return "(?)"
	}
}

// ---------------- Handlers ----------------
func (h *consumerHandler) handleMahasiswa(before, after gjson.Result, op *string) {
	if after.Exists() {
		*op = "MAHASISWA_UPSERT"

		kodeFak := after.Get("kode_fak").String()
		kodeProdi := after.Get("kode_prodi").String()

		// ambil nama fakultas & prodi dari redis
		namaFak := ""
		if val, err := rdb.Get(ctxBg, "fakultas#"+kodeFak).Result(); err == nil {
			namaFak = gjson.Get(val, "nama_fakultas").String()
		} else if err != redis.Nil {
			log.Printf("⚠️ redis error fakultas %s: %v", kodeFak, err)
		}

		namaProdi := ""
		if val, err := rdb.Get(ctxBg, "prodi#"+kodeProdi).Result(); err == nil {
			namaProdi = gjson.Get(val, "nama_prodi").String()
		} else if err != redis.Nil {
			log.Printf("⚠️ redis error prodi %s: %v", kodeProdi, err)
		}

		d := MahasiswaJoined{
			NIM:           after.Get("NIM").String(),
			KodeFak:       kodeFak,
			NamaFakultas:  namaFak,
			NamaFakultasMap1: namaFak + " " + mapJenjangV1(after.Get("kode_jenjang").String()),
			NamaFakultasMap2: namaFak + " " + mapJenjangV2(after.Get("kode_jenjang").String()),

			KodeJurusan:   after.Get("kode_jurusan").String(),
			KodeJenjang:   after.Get("kode_jenjang").String(),
			JenjangMap1:   mapJenjangV1(after.Get("kode_jenjang").String()),
			JenjangMap2:   mapJenjangV2(after.Get("kode_jenjang").String()),

			KodeProdi:     kodeProdi,
			NamaProdi:     namaProdi,
			NamaProdiMap1: namaProdi + " " + mapJenjangV1(after.Get("kode_jenjang").String()),
			NamaProdiMap2: namaProdi + " " + mapJenjangV2(after.Get("kode_jenjang").String()),
			ProdiFakultas1: namaProdi + " [" + namaFak + "]",
			ProdiFakultas2: namaProdi + " [" + namaFak + "] " + mapJenjangV2(after.Get("kode_jenjang").String()),

			NamaMahasiswa: after.Get("nama_mahasiswa").String(),
			StatusAktif:   after.Get("status_aktif").String(),
			TahunMasuk:    after.Get("tahun_masuk").String(),
		}

		if b, err := json.MarshalIndent(d, "", "  "); err == nil {
			fmt.Println("Upsert:\n" + string(b))
		} else {
			log.Printf("❌ Upsert: %v", err)
		}

		// contoh jika mau insert ke MariaDB
		q := fmt.Sprintf(`INSERT INTO %s 
			(nim, kode_fak, nama_fakultas, nama_fakultas_map1, nama_fakultas_map2,
			 kode_jurusan, kode_jenjang, jenjang_map1, jenjang_map2,
			 kode_prodi, nama_prodi, nama_prodi_map1, nama_prodi_map2,
			 nama_mahasiswa, status_aktif, tahun_masuk,
			 prodi_fakultas1, prodi_fakultas2)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
			ON DUPLICATE KEY UPDATE
				kode_fak=VALUES(kode_fak),
				nama_fakultas=VALUES(nama_fakultas),
				nama_fakultas_map1=VALUES(nama_fakultas_map1),
				nama_fakultas_map2=VALUES(nama_fakultas_map2),
				kode_jurusan=VALUES(kode_jurusan),
				kode_jenjang=VALUES(kode_jenjang),
				jenjang_map1=VALUES(jenjang_map1),
				jenjang_map2=VALUES(jenjang_map2),
				kode_prodi=VALUES(kode_prodi),
				nama_prodi=VALUES(nama_prodi),
				nama_prodi_map1=VALUES(nama_prodi_map1),
				nama_prodi_map2=VALUES(nama_prodi_map2),
				nama_mahasiswa=VALUES(nama_mahasiswa),
				status_aktif=VALUES(status_aktif),
				tahun_masuk=VALUES(tahun_masuk),
				prodi_fakultas1=VALUES(prodi_fakultas1),
				prodi_fakultas2=VALUES(prodi_fakultas2)`, h.tbl)

		_, err := dbSQL.Exec(q,
			d.NIM,
			d.KodeFak, d.NamaFakultas, d.NamaFakultasMap1, d.NamaFakultasMap2,
			d.KodeJurusan, d.KodeJenjang, d.JenjangMap1, d.JenjangMap2,
			d.KodeProdi, d.NamaProdi, d.NamaProdiMap1, d.NamaProdiMap2,
			d.NamaMahasiswa, d.StatusAktif, d.TahunMasuk,
			d.ProdiFakultas1, d.ProdiFakultas2,
		)
		if err != nil {
			log.Printf("❌ upsert mahasiswa (denom): %v", err)
			return
		}
		if b, err := json.MarshalIndent(d, "", "  "); err == nil {
			fmt.Println("Upsert:\n" + string(b))
		} else {
			log.Printf("❌ marshal mahasiswa: %v", err)
		}
	} else if before.Exists() {
		*op = "MAHASISWA_DELETE"
		nidn := before.Get("NIDN").String()
		fmt.Println("Delete:\n" + nidn)

		q := fmt.Sprintf(`DELETE FROM %s WHERE nidn=?`, h.tbl)
		if _, err := dbSQL.Exec(q, nidn); err != nil {
			log.Printf("❌ delete mahasiswa: %v", err)
		}
	}
}

// ---------------- MariaDB ----------------
func initMariaDB() {
	dsn := mustEnv("DSN","")
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
