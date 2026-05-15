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
	"sync/atomic"
	"syscall"
	"time"

	"github.com/EchoMessenger/router/internal/config"
	pbx "github.com/EchoMessenger/router/pbx"
	"github.com/IBM/sarama"
	"github.com/xdg-go/scram"

	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"
	"google.golang.org/protobuf/proto"
)

// ============================================================
// Константы
// ============================================================

const (
	kafkaSendTimeout = 5 * time.Second

	// Kafka topic names (без префикса)
	topicAccountEvents      = "account-events"
	topicTopicEvents        = "topic-events"
	topicSubscriptionEvents = "subscription-events"
	topicMessageEvents      = "message-events"
	topicSearchQueries      = "search-queries"
	topicDLQ                = "dlq"

	topicFirehoseHandshake     = "firehose.handshake"
	topicFirehoseAccountMgmt   = "firehose.account-mgmt"
	topicFirehoseAuth          = "firehose.auth"
	topicFirehoseSubscriptions = "firehose.subscriptions"
	topicFirehoseMessages      = "firehose.messages"
	topicFirehoseQueries       = "firehose.queries"
	topicFirehoseUpdates       = "firehose.updates"
	topicFirehoseDeletions     = "firehose.deletions"
	topicFirehoseNotifications = "firehose.notifications"
)

// ============================================================
// SCRAM client
// ============================================================

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

func (x *XDGSCRAMClient) Step(challenge string) (string, error) {
	return x.ClientConversation.Step(challenge)
}

func (x *XDGSCRAMClient) Done() bool {
	return x.ClientConversation.Done()
}

// ============================================================
// Kafka producer
// ============================================================

type Kafka struct {
	prod       sarama.AsyncProducer
	prefix     string
	log        *slog.Logger
	sentCount  atomic.Int64
	ackCount   atomic.Int64
	errorCount atomic.Int64
}

func newKafka(brokers []string, clientID, prefix string, log *slog.Logger, tune func(*sarama.Config)) (*Kafka, error) {
	log.Info("kafka.init.start",
		"brokers", brokers,
		"client_id", clientID,
		"topic_prefix", prefix,
	)

	cfg := sarama.NewConfig()
	cfg.ClientID = clientID
	cfg.Producer.Return.Successes = true
	cfg.Producer.Idempotent = true
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Net.MaxOpenRequests = 1
	cfg.Producer.Retry.Max = 5
	cfg.Producer.Retry.Backoff = 200 * time.Millisecond

	if tune != nil {
		tune(cfg)
	}

	log.Info("kafka.init.config",
		"tls_enabled", cfg.Net.TLS.Enable,
		"sasl_enabled", cfg.Net.SASL.Enable,
		"sasl_mechanism", string(cfg.Net.SASL.Mechanism),
		"idempotent", cfg.Producer.Idempotent,
		"required_acks", cfg.Producer.RequiredAcks,
		"retry_max", cfg.Producer.Retry.Max,
	)

	t := time.Now()
	prod, err := sarama.NewAsyncProducer(brokers, cfg)
	if err != nil {
		log.Error("kafka.init.failed",
			"err", err,
			"brokers", brokers,
			"elapsed_ms", time.Since(t).Milliseconds(),
		)
		return nil, err
	}

	log.Info("kafka.init.ok",
		"elapsed_ms", time.Since(t).Milliseconds(),
	)

	k := &Kafka{
		prod:   prod,
		prefix: strings.TrimSuffix(prefix, "."),
		log:    log,
	}

	// Горутина обработки acks и ошибок от async producer
	go func() {
		for {
			select {
			case msg, ok := <-prod.Successes():
				if !ok {
					log.Warn("kafka.ack_channel.closed")
					return
				}
				k.ackCount.Add(1)
				log.Debug("kafka.ack",
					"topic", msg.Topic,
					"partition", msg.Partition,
					"offset", msg.Offset,
				)

			case err, ok := <-prod.Errors():
				if !ok {
					log.Warn("kafka.error_channel.closed")
					return
				}
				k.errorCount.Add(1)
				log.Error("kafka.send.error",
					"topic", err.Msg.Topic,
					"partition_key", err.Msg.Key,
					"err", err.Err,
					"total_errors", k.errorCount.Load(),
				)
			}
		}
	}()

	// Периодическая статистика продюсера
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			sent := k.sentCount.Load()
			acks := k.ackCount.Load()
			errs := k.errorCount.Load()
			log.Info("kafka.stats",
				"sent", sent,
				"acks", acks,
				"errors", errs,
				"pending", sent-acks-errs,
			)
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

// send отправляет сообщение в Kafka.
// Таймаут инкапсулирован внутри — вызывающий код сигнатуру не меняет.
func (k *Kafka) send(topic, key string, value []byte) error {
	fullTopic := k.topic(topic)

	msg := &sarama.ProducerMessage{
		Topic: fullTopic,
		Key:   sarama.StringEncoder(key),
		Value: sarama.ByteEncoder(value),
	}

	ctx, cancel := context.WithTimeout(context.Background(), kafkaSendTimeout)
	defer cancel()

	select {
	case k.prod.Input() <- msg:
		k.sentCount.Add(1)
		k.log.Debug("kafka.send.queued",
			"topic", fullTopic,
			"partition_key", key,
			"payload_bytes", len(value),
		)
		return nil
	case <-ctx.Done():
		k.log.Error("kafka.send.timeout",
			"topic", fullTopic,
			"partition_key", key,
			"timeout_ms", kafkaSendTimeout.Milliseconds(),
		)
		return ctx.Err()
	}
}

// sendToDLQ отправляет сырые байты в DLQ с указанием причины.
func (k *Kafka) sendToDLQ(key string, value []byte, reason string) {
	k.log.Warn("kafka.dlq.send",
		"partition_key", key,
		"reason", reason,
		"payload_bytes", len(value),
	)
	// Ошибку DLQ не обрабатываем — если DLQ тоже недоступен, теряем событие,
	// но не блокируем основной поток.
	_ = k.send(topicDLQ, key, value)
}

// ============================================================
// Helpers
// ============================================================

// redactSecrets обнуляет credentials в ClientMsg перед сериализацией.
// Вызывается до proto.Marshal, мутирует переданный объект.
func redactSecrets(msg *pbx.ClientMsg) {
	if msg == nil {
		return
	}
	if acc := msg.GetAcc(); acc != nil {
		acc.Secret = nil
		acc.TmpSecret = nil
	}
	if login := msg.GetLogin(); login != nil {
		login.Secret = nil
	}
}

func getClientAddr(ctx context.Context) string {
	if p, ok := peer.FromContext(ctx); ok {
		return p.Addr.String()
	}
	return "unknown"
}

// ============================================================
// gRPC Plugin server
// ============================================================

type pluginServer struct {
	pbx.UnimplementedPluginServer
	kafka     *Kafka
	log       *slog.Logger
	startTime time.Time

	fireHoseCount     atomic.Int64
	findCount         atomic.Int64
	accountCount      atomic.Int64
	topicCount        atomic.Int64
	subscriptionCount atomic.Int64
	messageCount      atomic.Int64
}

// ---- FireHose ----

func (s *pluginServer) FireHose(ctx context.Context, in *pbx.ClientReq) (*pbx.ServerResp, error) {
	callNum := s.fireHoseCount.Add(1)
	clientAddr := getClientAddr(ctx)

	if in == nil {
		s.log.Warn("firehose.nil_input", "call", callNum, "client", clientAddr)
		return &pbx.ServerResp{Status: pbx.RespCode_CONTINUE}, nil
	}

	sess := in.GetSess()
	msg := in.GetMsg()

	if msg == nil {
		s.log.Warn("firehose.nil_msg",
			"call", callNum,
			"client", clientAddr,
			"user_id", sess.GetUserId(),
			"session_id", sess.GetSessionId(),
		)
		return &pbx.ServerResp{Status: pbx.RespCode_CONTINUE}, nil
	}

	userId := sess.GetUserId()
	msgType, kafkaTopic, partitionKey := s.classifyMessage(msg, userId)

	s.log.Debug("firehose.recv",
		"call", callNum,
		"msg_type", msgType,
		"topic", kafkaTopic,
		"partition_key", partitionKey,
		"user_id", userId,
		"session_id", sess.GetSessionId(),
		"auth_level", sess.GetAuthLevel().String(),
		"remote_addr", sess.GetRemoteAddr(),
		"device_id", sess.GetDeviceId(),
	)

	// Зачищаем секреты до сериализации
	redactSecrets(msg)

	val, err := proto.Marshal(in)
	if err != nil {
		s.log.Error("firehose.marshal.failed",
			"call", callNum,
			"msg_type", msgType,
			"user_id", userId,
			"err", err,
		)
		return &pbx.ServerResp{Status: pbx.RespCode_CONTINUE}, nil
	}

	if err := s.kafka.send(kafkaTopic, partitionKey, val); err != nil {
		s.log.Error("firehose.kafka.failed",
			"call", callNum,
			"msg_type", msgType,
			"kafka_topic", kafkaTopic,
			"partition_key", partitionKey,
			"user_id", userId,
			"err", err,
		)
		s.kafka.sendToDLQ(partitionKey, val, fmt.Sprintf("firehose send failed: %v", err))
	}

	return &pbx.ServerResp{Status: pbx.RespCode_CONTINUE}, nil
}

// classifyMessage определяет тип сообщения, целевой Kafka топик и ключ партиционирования.
func (s *pluginServer) classifyMessage(msg *pbx.ClientMsg, userID string) (msgType, kafkaTopic, partitionKey string) {
	switch {
	case msg.GetHi() != nil:
		return "hi", topicFirehoseHandshake, userID

	case msg.GetAcc() != nil:
		acc := msg.GetAcc()
		target := acc.GetUserId()
		if target == "" {
			target = userID
		}
		return "acc", topicFirehoseAccountMgmt, target

	case msg.GetLogin() != nil:
		return "login", topicFirehoseAuth, userID

	case msg.GetSub() != nil:
		return "sub", topicFirehoseSubscriptions, msg.GetSub().GetTopic()

	case msg.GetLeave() != nil:
		return "leave", topicFirehoseSubscriptions, msg.GetLeave().GetTopic()

	case msg.GetPub() != nil:
		return "pub", topicFirehoseMessages, msg.GetPub().GetTopic()

	case msg.GetGet() != nil:
		return "get", topicFirehoseQueries, userID

	case msg.GetSet() != nil:
		return "set", topicFirehoseUpdates, msg.GetSet().GetTopic()

	case msg.GetDel() != nil:
		return "del", topicFirehoseDeletions, msg.GetDel().GetTopic()

	case msg.GetNote() != nil:
		return "note", topicFirehoseNotifications, msg.GetNote().GetTopic()

	default:
		return "unknown", topicDLQ, userID
	}
}

// ---- Find ----

func (s *pluginServer) Find(ctx context.Context, in *pbx.SearchQuery) (*pbx.SearchFound, error) {
	callNum := s.findCount.Add(1)
	clientAddr := getClientAddr(ctx)

	if in == nil {
		s.log.Warn("find.nil_input", "call", callNum, "client", clientAddr)
		return &pbx.SearchFound{Status: pbx.RespCode_CONTINUE}, nil
	}

	s.log.Debug("find.recv",
		"call", callNum,
		"user_id", in.GetUserId(),
		"query_len", len(in.GetQuery()),
	)

	val, err := proto.Marshal(in)
	if err != nil {
		s.log.Error("find.marshal.failed",
			"call", callNum,
			"user_id", in.GetUserId(),
			"err", err,
		)
		return &pbx.SearchFound{Status: pbx.RespCode_CONTINUE}, nil
	}

	if err := s.kafka.send(topicSearchQueries, in.GetUserId(), val); err != nil {
		s.log.Error("find.kafka.failed",
			"call", callNum,
			"user_id", in.GetUserId(),
			"err", err,
		)
	}

	return &pbx.SearchFound{Status: pbx.RespCode_CONTINUE}, nil
}

// ---- Account ----

func (s *pluginServer) Account(ctx context.Context, in *pbx.AccountEvent) (*pbx.Unused, error) {
	callNum := s.accountCount.Add(1)
	clientAddr := getClientAddr(ctx)

	if in == nil {
		s.log.Error("account.nil_input", "call", callNum, "client", clientAddr)
		return &pbx.Unused{}, errors.New("nil AccountEvent")
	}

	s.log.Debug("account.recv",
		"call", callNum,
		"user_id", in.GetUserId(),
		"action", in.GetAction().String(),
		"tags_count", len(in.GetTags()),
	)

	val, err := proto.Marshal(in)
	if err != nil {
		s.log.Error("account.marshal.failed",
			"call", callNum,
			"user_id", in.GetUserId(),
			"err", err,
		)
		return &pbx.Unused{}, err
	}

	if err := s.kafka.send(topicAccountEvents, in.GetUserId(), val); err != nil {
		s.log.Error("account.kafka.failed",
			"call", callNum,
			"user_id", in.GetUserId(),
			"action", in.GetAction().String(),
			"err", err,
		)
		s.kafka.sendToDLQ(in.GetUserId(), val, fmt.Sprintf("account send failed: %v", err))
	}

	return &pbx.Unused{}, nil
}

// ---- Topic ----

func (s *pluginServer) Topic(ctx context.Context, in *pbx.TopicEvent) (*pbx.Unused, error) {
	callNum := s.topicCount.Add(1)
	clientAddr := getClientAddr(ctx)

	if in == nil {
		s.log.Error("topic.nil_input", "call", callNum, "client", clientAddr)
		return &pbx.Unused{}, errors.New("nil TopicEvent")
	}

	s.log.Debug("topic.recv",
		"call", callNum,
		"topic_name", in.GetName(),
		"action", in.GetAction().String(),
	)

	val, err := proto.Marshal(in)
	if err != nil {
		s.log.Error("topic.marshal.failed",
			"call", callNum,
			"topic_name", in.GetName(),
			"err", err,
		)
		return &pbx.Unused{}, err
	}

	if err := s.kafka.send(topicTopicEvents, in.GetName(), val); err != nil {
		s.log.Error("topic.kafka.failed",
			"call", callNum,
			"topic_name", in.GetName(),
			"action", in.GetAction().String(),
			"err", err,
		)
		s.kafka.sendToDLQ(in.GetName(), val, fmt.Sprintf("topic send failed: %v", err))
	}

	return &pbx.Unused{}, nil
}

// ---- Subscription ----

func (s *pluginServer) Subscription(ctx context.Context, in *pbx.SubscriptionEvent) (*pbx.Unused, error) {
	callNum := s.subscriptionCount.Add(1)
	clientAddr := getClientAddr(ctx)

	if in == nil {
		s.log.Error("subscription.nil_input", "call", callNum, "client", clientAddr)
		return &pbx.Unused{}, errors.New("nil SubscriptionEvent")
	}

	s.log.Debug("subscription.recv",
		"call", callNum,
		"topic", in.GetTopic(),
		"user_id", in.GetUserId(),
		"action", in.GetAction().String(),
	)

	val, err := proto.Marshal(in)
	if err != nil {
		s.log.Error("subscription.marshal.failed",
			"call", callNum,
			"topic", in.GetTopic(),
			"user_id", in.GetUserId(),
			"err", err,
		)
		return &pbx.Unused{}, err
	}

	if err := s.kafka.send(topicSubscriptionEvents, in.GetTopic(), val); err != nil {
		s.log.Error("subscription.kafka.failed",
			"call", callNum,
			"topic", in.GetTopic(),
			"user_id", in.GetUserId(),
			"action", in.GetAction().String(),
			"err", err,
		)
		s.kafka.sendToDLQ(in.GetTopic(), val, fmt.Sprintf("subscription send failed: %v", err))
	}

	return &pbx.Unused{}, nil
}

// ---- Message ----

func (s *pluginServer) Message(ctx context.Context, in *pbx.MessageEvent) (*pbx.Unused, error) {
	callNum := s.messageCount.Add(1)
	clientAddr := getClientAddr(ctx)

	if in == nil {
		s.log.Error("message.nil_input", "call", callNum, "client", clientAddr)
		return &pbx.Unused{}, errors.New("nil MessageEvent")
	}

	msg := in.GetMsg()
	s.log.Debug("message.recv",
		"call", callNum,
		"action", in.GetAction().String(),
		"topic", msg.GetTopic(),
		"from_user_id", msg.GetFromUserId(),
		"seq_id", msg.GetSeqId(),
		"content_bytes", len(msg.GetContent()),
	)

	val, err := proto.Marshal(in)
	if err != nil {
		s.log.Error("message.marshal.failed",
			"call", callNum,
			"topic", msg.GetTopic(),
			"seq_id", msg.GetSeqId(),
			"err", err,
		)
		return &pbx.Unused{}, err
	}

	if err := s.kafka.send(topicMessageEvents, msg.GetTopic(), val); err != nil {
		s.log.Error("message.kafka.failed",
			"call", callNum,
			"topic", msg.GetTopic(),
			"from_user_id", msg.GetFromUserId(),
			"seq_id", msg.GetSeqId(),
			"err", err,
		)
		s.kafka.sendToDLQ(msg.GetTopic(), val, fmt.Sprintf("message send failed: %v", err))
	}

	return &pbx.Unused{}, nil
}

// ============================================================
// gRPC interceptor
// ============================================================

func loggingInterceptor(log *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		t := time.Now()
		clientAddr := getClientAddr(ctx)

		resp, err := handler(ctx, req)

		attrs := []any{
			"grpc_method", info.FullMethod,
			"client", clientAddr,
			"elapsed_ms", time.Since(t).Milliseconds(),
		}
		if err != nil {
			attrs = append(attrs, "err", err)
			log.Error("grpc.call.error", attrs...)
		} else {
			log.Debug("grpc.call.ok", attrs...)
		}

		return resp, err
	}
}

// ============================================================
// main
// ============================================================

func main() {
	startTime := time.Now()

	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintf(os.Stderr, "config load failed: %v\n", err)
		os.Exit(1)
	}

	level := slog.LevelInfo
	switch strings.ToLower(cfg.LogLevel) {
	case "debug":
		level = slog.LevelDebug
	case "warn", "warning":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	}

	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: level,
	}))
	slog.SetDefault(log)

	log.Info("service.start",
		"pid", os.Getpid(),
		"log_level", level.String(),
		"grpc_addr", cfg.ListenAddr,
		"health_addr", cfg.HealthAddr,
		"kafka_brokers", cfg.KafkaBrokers,
		"topic_prefix", cfg.TopicPrefix,
		"kafka_tls", cfg.KafkaTLSEnable,
		"kafka_sasl", cfg.KafkaSASLEnable,
		"kafka_sasl_mechanism", cfg.KafkaSASLMechanism,
		"kafka_sasl_user", cfg.KafkaSASLUsername,
	)

	k, err := newKafka(cfg.KafkaBrokers, cfg.KafkaClientID, cfg.TopicPrefix, log, func(sc *sarama.Config) {
		if cfg.KafkaTLSEnable {
			log.Info("kafka.tls.enabled")
			sc.Net.TLS.Enable = true
			sc.Net.TLS.Config = &tls.Config{
				InsecureSkipVerify: cfg.KafkaTLSInsecureSkipVerify,
			}
		}
		if cfg.KafkaSASLEnable {
			log.Info("kafka.sasl.enabled",
				"mechanism", cfg.KafkaSASLMechanism,
				"user", cfg.KafkaSASLUsername,
			)
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
				log.Warn("kafka.sasl.unknown_mechanism",
					"mechanism", cfg.KafkaSASLMechanism,
					"fallback", "PLAIN",
				)
				sc.Net.SASL.Mechanism = sarama.SASLTypePlaintext
			}
		}
	})
	if err != nil {
		log.Error("kafka.init.fatal", "err", err)
		os.Exit(1)
	}

	grpcServer := grpc.NewServer(
		grpc.UnaryInterceptor(loggingInterceptor(log)),
	)

	pluginSrv := &pluginServer{
		kafka:     k,
		log:       log,
		startTime: startTime,
	}
	pbx.RegisterPluginServer(grpcServer, pluginSrv)

	listener, err := net.Listen("tcp", cfg.ListenAddr)
	if err != nil {
		log.Error("grpc.listen.failed", "addr", cfg.ListenAddr, "err", err)
		os.Exit(1)
	}

	log.Info("grpc.listen.ok", "addr", cfg.ListenAddr)

	go func() {
		if err := grpcServer.Serve(listener); err != nil {
			log.Error("grpc.serve.failed", "err", err)
			os.Exit(1)
		}
	}()

	// Health + stats HTTP
	go func() {
		mux := http.NewServeMux()

		mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok"))
		})

		mux.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
			sent := k.sentCount.Load()
			acks := k.ackCount.Load()
			errs := k.errorCount.Load()

			stats := fmt.Sprintf(`{
  "uptime_seconds": %d,
  "kafka": {
    "sent": %d,
    "acks": %d,
    "errors": %d,
    "pending": %d
  },
  "grpc": {
    "firehose_calls": %d,
    "find_calls": %d,
    "account_calls": %d,
    "topic_calls": %d,
    "subscription_calls": %d,
    "message_calls": %d
  }
}`,
				int(time.Since(startTime).Seconds()),
				sent, acks, errs, sent-acks-errs,
				pluginSrv.fireHoseCount.Load(),
				pluginSrv.findCount.Load(),
				pluginSrv.accountCount.Load(),
				pluginSrv.topicCount.Load(),
				pluginSrv.subscriptionCount.Load(),
				pluginSrv.messageCount.Load(),
			)

			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(stats))
		})

		log.Info("health.listen.ok", "addr", cfg.HealthAddr)
		if err := http.ListenAndServe(cfg.HealthAddr, mux); err != nil {
			log.Error("health.serve.failed", "err", err)
		}
	}()

	// Периодический heartbeat
	go func() {
		ticker := time.NewTicker(60 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			sent := k.sentCount.Load()
			acks := k.ackCount.Load()
			errs := k.errorCount.Load()
			log.Info("service.heartbeat",
				"uptime_seconds", int(time.Since(startTime).Seconds()),
				"kafka_sent", sent,
				"kafka_acks", acks,
				"kafka_errors", errs,
				"kafka_pending", sent-acks-errs,
				"grpc_calls_total",
				pluginSrv.fireHoseCount.Load()+
					pluginSrv.findCount.Load()+
					pluginSrv.accountCount.Load()+
					pluginSrv.topicCount.Load()+
					pluginSrv.subscriptionCount.Load()+
					pluginSrv.messageCount.Load(),
			)
		}
	}()

	log.Info("service.ready",
		"grpc_addr", cfg.ListenAddr,
		"health_addr", cfg.HealthAddr,
	)

	// Graceful shutdown
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	received := <-sig

	log.Info("service.shutdown.start", "signal", received.String())

	grpcServer.GracefulStop()
	log.Info("service.shutdown.grpc.ok")

	if err := k.prod.Close(); err != nil {
		log.Error("service.shutdown.kafka.failed", "err", err)
	} else {
		log.Info("service.shutdown.kafka.ok")
	}

	log.Info("service.shutdown.complete",
		"uptime_seconds", int(time.Since(startTime).Seconds()),
		"kafka_sent", k.sentCount.Load(),
		"kafka_acks", k.ackCount.Load(),
		"kafka_errors", k.errorCount.Load(),
	)
}