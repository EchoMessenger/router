package main

import (
	"context"
	"crypto/sha256"
	"crypto/sha512"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/EchoMessenger/router/internal/config"
	pbx "github.com/EchoMessenger/router/pbx"
	"github.com/IBM/sarama"
	"github.com/xdg-go/scram"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

// ---- SCRAM client generator ----

var (
	SHA256 scram.HashGeneratorFcn = sha256.New
	SHA512 scram.HashGeneratorFcn = sha512.New
)

type XDGSCRAMClient struct {
	*scram.Client
	*scram.ClientConversation
	scram.HashGeneratorFcn
}

func (x *XDGSCRAMClient) Begin(userName, password, authzID string) (err error) {
	x.Client, err = x.HashGeneratorFcn.NewClient(userName, password, authzID)
	if err != nil {
		return err
	}
	x.ClientConversation = x.Client.NewConversation()
	return nil
}

func (x *XDGSCRAMClient) Step(challenge string) (response string, err error) {
	response, err = x.ClientConversation.Step(challenge)
	return
}

func (x *XDGSCRAMClient) Done() bool {
	return x.ClientConversation.Done()
}

// ---- Kafka wrapper ----

type Kafka struct {
	prod   sarama.AsyncProducer
	prefix string
	log    *slog.Logger
}

func newKafka(brokers []string, clientID, prefix string, log *slog.Logger, tune func(*sarama.Config)) (*Kafka, error) {
	cfg := sarama.NewConfig()
	cfg.ClientID = clientID
	cfg.Producer.Return.Successes = true
	cfg.Producer.Idempotent = true
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Net.MaxOpenRequests = 1
	cfg.Producer.Retry.Max = 5
	cfg.Producer.Retry.Backoff = 200 * time.Millisecond

	// применим внешние настройки (SASL/TLS и т.п.)
	if tune != nil {
		tune(cfg)
	}

	prod, err := sarama.NewAsyncProducer(brokers, cfg)
	if err != nil {
		return nil, err
	}

	k := &Kafka{prod: prod, prefix: strings.TrimSuffix(prefix, "."), log: log}

	go func() {
		for {
			select {
			case msg := <-prod.Successes():
				if msg != nil {
					log.Debug("kafka ack", "topic", msg.Topic)
				}
			case err := <-prod.Errors():
				if err != nil {
					log.Error("kafka error", "topic", err.Msg.Topic, "err", err.Err)
				}
			}
		}
	}()

	return k, nil
}

func (k *Kafka) topic(name string) string {
	if k.prefix == "" {
		return name
	}
	return k.prefix + "." + name
}

func (k *Kafka) send(topic, key string, value []byte) {
	msg := &sarama.ProducerMessage{
		Topic: k.topic(topic),
		Key:   sarama.StringEncoder(key),
		Value: sarama.ByteEncoder(value),
	}
	k.prod.Input() <- msg
}

// ---- gRPC Plugin server ----

type pluginServer struct {
	pbx.UnimplementedPluginServer
	kafka *Kafka
	log   *slog.Logger
}

func (s *pluginServer) FireHose(ctx context.Context, in *pbx.ClientReq) (*pbx.ServerResp, error) {
	s.log.Debug("FireHose", "have_msg", in != nil && in.GetMsg() != nil)
	if in != nil {
		key := in.GetSess().GetUserId()
		val, _ := proto.Marshal(in)
		s.kafka.send("client-req", key, val)
	}
	return &pbx.ServerResp{Status: pbx.RespCode_CONTINUE}, nil
}

func (s *pluginServer) Find(ctx context.Context, in *pbx.SearchQuery) (*pbx.SearchFound, error) {
	return &pbx.SearchFound{Status: pbx.RespCode_CONTINUE}, nil
}

func (s *pluginServer) Account(ctx context.Context, in *pbx.AccountEvent) (*pbx.Unused, error) {
	if in == nil {
		return &pbx.Unused{}, errors.New("nil AccountEvent")
	}
	key := in.GetUserId()
	val, _ := proto.Marshal(in)
	s.kafka.send("account-events", key, val)
	return &pbx.Unused{}, nil
}

func (s *pluginServer) Topic(ctx context.Context, in *pbx.TopicEvent) (*pbx.Unused, error) {
	if in == nil {
		return &pbx.Unused{}, errors.New("nil TopicEvent")
	}
	key := in.GetName()
	val, _ := proto.Marshal(in)
	s.kafka.send("topic-events", key, val)
	return &pbx.Unused{}, nil
}

func (s *pluginServer) Subscription(ctx context.Context, in *pbx.SubscriptionEvent) (*pbx.Unused, error) {
	if in == nil {
		return &pbx.Unused{}, errors.New("nil SubscriptionEvent")
	}
	key := fmt.Sprintf("%s:%s", in.GetTopic(), in.GetUserId())
	val, _ := proto.Marshal(in)
	s.kafka.send("subscription-events", key, val)
	return &pbx.Unused{}, nil
}

func (s *pluginServer) Message(ctx context.Context, in *pbx.MessageEvent) (*pbx.Unused, error) {
	if in == nil {
		return &pbx.Unused{}, errors.New("nil MessageEvent")
	}
	key := "message"
	val, _ := proto.Marshal(in)
	s.kafka.send("message-events", key, val)
	return &pbx.Unused{}, nil
}

// ---- bootstrap & health ----

func main() {
	// 1) грузим конфиг из .env/окружения
	cfg, err := config.Load()
	if err != nil {
		panic(err)
	}

	// 2) настраиваем логгер по уровню
	level := slog.LevelInfo
	switch strings.ToLower(cfg.LogLevel) {
	case "debug":
		level = slog.LevelDebug
	case "warn", "warning":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	}
	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: level}))

	// 3) kafka producer с учётом SASL/TLS из конфига
	k, err := newKafka(cfg.KafkaBrokers, cfg.KafkaClientID, cfg.TopicPrefix, log, func(sc *sarama.Config) {
		// TLS
		if cfg.KafkaTLSEnable {
			sc.Net.TLS.Enable = true
			sc.Net.TLS.Config = &tls.Config{InsecureSkipVerify: cfg.KafkaTLSInsecureSkipVerify}
		}
		// SASL
		if cfg.KafkaSASLEnable {
			sc.Net.SASL.Enable = true
			sc.Net.SASL.User = cfg.KafkaSASLUsername
			sc.Net.SASL.Password = cfg.KafkaSASLPassword

			switch strings.ToUpper(cfg.KafkaSASLMechanism) {
			case "PLAIN":
				sc.Net.SASL.Mechanism = sarama.SASLTypePlaintext
			case "SCRAM-SHA-256":
				sc.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA256
				sc.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
					return &XDGSCRAMClient{HashGeneratorFcn: SHA256}
				}
			case "SCRAM-SHA-512":
				sc.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
				sc.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
					return &XDGSCRAMClient{HashGeneratorFcn: SHA512}
				}
			default:
				log.Warn("unknown SASL mechanism, fallback to PLAIN", "mech", cfg.KafkaSASLMechanism)
				sc.Net.SASL.Mechanism = sarama.SASLTypePlaintext
			}
		}
	})
	if err != nil {
		log.Error("kafka init failed", "err", err)
		os.Exit(1)
	}

	// 4) gRPC сервер
	s := grpc.NewServer()
	pbx.RegisterPluginServer(s, &pluginServer{kafka: k, log: log})

	l, err := net.Listen("tcp", cfg.ListenAddr)
	if err != nil {
		log.Error("listen failed", "addr", cfg.ListenAddr, "err", err)
		os.Exit(1)
	}

	go func() {
		log.Info("grpc listening", "addr", cfg.ListenAddr)
		if err := s.Serve(l); err != nil {
			log.Error("grpc serve failed", "err", err)
			os.Exit(1)
		}
	}()

	// Healthcheck
	go func() {
		mux := http.NewServeMux()
		mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok")) })
		_ = http.ListenAndServe(cfg.HealthAddr, mux)
	}()

	// Graceful shutdown
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	log.Info("shutting down...")
	s.GracefulStop()
	_ = k.prod.Close()
}
