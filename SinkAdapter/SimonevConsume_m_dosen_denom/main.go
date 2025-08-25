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
	"github.com/tecbot/gorocksdb"
	"github.com/tidwall/gjson"
	_ "github.com/go-sql-driver/mysql"
)

const (
	defaultBrokers       = "kafka:9092"
	defaultTopicFakultas = "simak2.unpak_simak.m_fakultas"
	defaultTopicProdi    = "simak4.unpak_simak.m_program_studi"
	defaultTopicDosen    = "simak5.unpak_simak.m_dosen"
)

var (
	db    *gorocksdb.DB
	dbSQL *sql.DB
)

// ---------------- Structs ----------------
type Fakultas struct {
	KodeFakultas string `json:"kode_fakultas"`
	NamaFakultas string `json:"nama_fakultas"`
}

type Prodi struct {
	KodeProdi string `json:"kode_prodi"`
	NamaProdi string `json:"nama_prodi"`
}

type Dosen struct {
	NIDN         string  `json:"nidn"`
	NIPLama      string  `json:"nip_lama"`
	NIPBaru      string  `json:"nip_baru"`
	NamaFakultas *string `json:"nama_fakultas,omitempty"`
	KodeFak      *string `json:"kode_fak,omitempty"`
	KodeJurusan  string  `json:"kode_jurusan"`
	NamaProdi    *string `json:"nama_prodi,omitempty"`
	KodeProdi    *string `json:"kode_prodi,omitempty"`
	KodeJenjang  string  `json:"kode_jenjang"`
	NamaDosen    string  `json:"nama_dosen"`
}

// ---------------- Utils ----------------
func mustEnv(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

// ---------------- Main ----------------
func main() {
	brokers := mustEnv("KAFKA_BROKERS", defaultBrokers)
	topicFakultas := mustEnv("TOPIC_FAKULTAS", defaultTopicFakultas)
	topicProdi := mustEnv("TOPIC_PRODI", defaultTopicProdi)
	topicDosen := mustEnv("TOPIC_DOSEN", defaultTopicDosen)
	rocksPath := mustEnv("ROCKS_PATH", "/data/rocksdb")

	log.Printf("▶️  starting joiner")
	log.Printf("    brokers      : %s", brokers)
	log.Printf("    topicFakultas: %s", topicFakultas)
	log.Printf("    topicProdi   : %s", topicProdi)
	log.Printf("    topicDosen   : %s", topicDosen)
	log.Printf("    rocksdb path : %s", rocksPath)

	// ---- Init RocksDB
	opts := gorocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)
	opts.SetIncreaseParallelism(2)
	opts.SetMaxBackgroundCompactions(2)
	var err error
	db, err = gorocksdb.OpenDb(opts, rocksPath)
	if err != nil {
		log.Fatalf("❌ failed to open rocksdb: %v", err)
	}
	defer db.Close()

	// ---- Init MariaDB
	initMariaDB()
	defer dbSQL.Close()

	// ---- Kafka configs for consuming only
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
		topicFakultas: topicFakultas,
		topicProdi:    topicProdi,
		topicDosen:    topicDosen,
	}

	go func() {
		for err := range consumer.Errors() {
			log.Printf("⚠️  consumer error: %v", err)
		}
	}()

	go func() {
		for {
			err := consumer.Consume(ctx, []string{topicFakultas, topicProdi, topicDosen}, handler)
			if err != nil {
				log.Printf("⚠️  consume error: %v", err)
				time.Sleep(time.Second)
			}
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	log.Println("🛑 shutting down joiner...")
	cancel()
}

// ---------------- Consumer Handler ----------------
type consumerHandler struct {
	topicFakultas string
	topicProdi    string
	topicDosen    string
}

func (h *consumerHandler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (h *consumerHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (h *consumerHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		begin := time.Now()
		op := "UNKNOWN"
		h.process(msg.Topic, msg.Value, &op)
		log.Printf("✅ handled topic=%s op=%s offset=%d in %s", msg.Topic, op, msg.Offset, time.Since(begin).Truncate(time.Millisecond))
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
	case h.topicFakultas:
		h.handleFakultas(before, after, op)
	case h.topicProdi:
		h.handleProdi(before, after, op)
	case h.topicDosen:
		h.handleDosen(before, after, op)
	default:
		*op = "SKIP"
	}
}

// ---------------- Handlers ----------------
func (h *consumerHandler) handleFakultas(before, after gjson.Result, op *string) {
	if after.Exists() {
		*op = "FAKULTAS_UPSERT"
		f := Fakultas{KodeFakultas: after.Get("kode_fakultas").String(), NamaFakultas: after.Get("nama_fakultas").String()}
		putStateFakultas(f)
	} else if before.Exists() {
		*op = "FAKULTAS_DELETE"
		deleteStateFakultas(before.Get("kode_fakultas").String())
	}
}

func (h *consumerHandler) handleProdi(before, after gjson.Result, op *string) {
	if after.Exists() {
		*op = "PRODI_UPSERT"
		p := Prodi{KodeProdi: after.Get("kode_prodi").String(), NamaProdi: after.Get("nama_prodi").String()}
		putStateProdi(p)
	} else if before.Exists() {
		*op = "PRODI_DELETE"
		deleteStateProdi(before.Get("kode_prodi").String())
	}
}

func (h *consumerHandler) handleDosen(before, after gjson.Result, op *string) {
	if after.Exists() {
		*op = "DOSEN_UPSERT"
		d := Dosen{
			NIDN:        after.Get("NIDN").String(),
			NIPLama:     after.Get("nip_lama").String(),
			NIPBaru:     after.Get("nip_baru").String(),
			KodeJurusan: after.Get("kode_jurusan").String(),
			KodeJenjang: after.Get("kode_jenjang").String(),
			NamaDosen:   after.Get("nama_dosen").String(),
		}
		kodeFak := after.Get("kode_fak").String()
		kodeProdi := after.Get("kode_prodi").String()
		if f, _ := readStateFakultas(kodeFak); f != nil {
			d.NamaFakultas = &f.NamaFakultas
			d.KodeFak = &f.KodeFakultas
		}
		if p, _ := readStateProdi(kodeProdi); p != nil {
			d.NamaProdi = &p.NamaProdi
			d.KodeProdi = &p.KodeProdi
		}
		putStateDosen(d)
		if output, err := json.MarshalIndent(d, "", "  "); err == nil {
			fmt.Println("Upsert: ")
			fmt.Println(string(output))
		} else {
			fmt.Println("Error upsert:", err)
		}
		// upsertDosenDB(d)
	} else if before.Exists() {
		*op = "DOSEN_DELETE"
		nidn := before.Get("NIDN").String()
		deleteStateDosen(nidn)

		fmt.Println("Delete: ", nidn)
		// deleteDosenDB(nidn)
	}
}

// ---------------- RocksDB ----------------
func putStateFakultas(f Fakultas) error {
	wo := gorocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()
	key := []byte("fakultas:" + f.KodeFakultas)
	b, _ := json.Marshal(f)
	return db.Put(wo, key, b)
}

func deleteStateFakultas(kode string) error {
	wo := gorocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()
	key := []byte("fakultas:" + kode)
	return db.Delete(wo, key)
}

func readStateFakultas(kode string) (*Fakultas, error) {
	ro := gorocksdb.NewDefaultReadOptions()
	defer ro.Destroy()
	key := []byte("fakultas:" + kode)
	v, err := db.Get(ro, key)
	if err != nil {
		return nil, err
	}
	defer v.Free()
	if !v.Exists() || v.Size() == 0 {
		return nil, nil
	}
	var f Fakultas
	json.Unmarshal(v.Data(), &f)
	return &f, nil
}

func putStateProdi(p Prodi) error {
	wo := gorocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()
	key := []byte("prodi:" + p.KodeProdi)
	b, _ := json.Marshal(p)
	return db.Put(wo, key, b)
}

func deleteStateProdi(kode string) error {
	wo := gorocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()
	key := []byte("prodi:" + kode)
	return db.Delete(wo, key)
}

func readStateProdi(kode string) (*Prodi, error) {
	ro := gorocksdb.NewDefaultReadOptions()
	defer ro.Destroy()
	key := []byte("prodi:" + kode)
	v, err := db.Get(ro, key)
	if err != nil {
		return nil, err
	}
	defer v.Free()
	if !v.Exists() || v.Size() == 0 {
		return nil, nil
	}
	var p Prodi
	json.Unmarshal(v.Data(), &p)
	return &p, nil
}

func putStateDosen(d Dosen) error {
	wo := gorocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()
	key := []byte("dosen:" + d.NIDN)
	b, _ := json.Marshal(d)
	return db.Put(wo, key, b)
}

func deleteStateDosen(nidn string) error {
	wo := gorocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()
	key := []byte("dosen:" + nidn)
	return db.Delete(wo, key)
}

// ---------------- MariaDB CRUD ----------------
func initMariaDB() {
	dsn := mustEnv("MYSQL_DSN", "root:password@tcp(localhost:3306)/testdb")
	var err error
	dbSQL, err = sql.Open("mysql", dsn)
	if err != nil {
		log.Fatalf("❌ mysql connect: %v", err)
	}
	if err = dbSQL.Ping(); err != nil {
		log.Fatalf("❌ mysql ping: %v", err)
	}
}

func upsertDosenDB(d Dosen) {
	q := `INSERT INTO dosen (nidn,nip_lama,nip_baru,nama_fakultas,kode_fak,kode_jurusan,nama_prodi,kode_prodi,kode_jenjang,nama_dosen)
	VALUES (?,?,?,?,?,?,?,?,?,?)
	ON DUPLICATE KEY UPDATE nip_lama=?,nip_baru=?,nama_fakultas=?,kode_fak=?,kode_jurusan=?,nama_prodi=?,kode_prodi=?,kode_jenjang=?,nama_dosen=?`
	_, _ = dbSQL.Exec(q,
		d.NIDN, d.NIPLama, d.NIPBaru, d.NamaFakultas, d.KodeFak, d.KodeJurusan, d.NamaProdi, d.KodeProdi, d.KodeJenjang, d.NamaDosen,
		d.NIPLama, d.NIPBaru, d.NamaFakultas, d.KodeFak, d.KodeJurusan, d.NamaProdi, d.KodeProdi, d.KodeJenjang, d.NamaDosen,
	)
}

func deleteDosenDB(nidn string) {
	q := `DELETE FROM dosen WHERE nidn=?`
	_, _ = dbSQL.Exec(q, nidn)
}
