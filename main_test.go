package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/EchoMessenger/router/internal/config"
	pbx "github.com/EchoMessenger/router/pbx"
	"github.com/IBM/sarama"
	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"
	"google.golang.org/protobuf/proto"
)

type sentMessage struct {
	topic string
	key   string
	value []byte
}

type fakeKafka struct {
	err  error
	sent []sentMessage
	dlq  []sentMessage
}

func (f *fakeKafka) send(topic, key string, value []byte) error {
	f.sent = append(f.sent, sentMessage{topic: topic, key: key, value: append([]byte(nil), value...)})
	return f.err
}

func (f *fakeKafka) sendToDLQ(key string, value []byte, reason string) {
	f.dlq = append(f.dlq, sentMessage{topic: topicDLQ, key: key, value: append([]byte(nil), value...)})
}

func testServer(k *fakeKafka) *pluginServer {
	return &pluginServer{kafka: k, log: slog.New(slog.NewTextHandler(io.Discard, nil))}
}

type fakeAsyncProducer struct {
	input     chan *sarama.ProducerMessage
	successes chan *sarama.ProducerMessage
	errors    chan *sarama.ProducerError
}

func newFakeAsyncProducer() *fakeAsyncProducer {
	return &fakeAsyncProducer{
		input:     make(chan *sarama.ProducerMessage, 1),
		successes: make(chan *sarama.ProducerMessage),
		errors:    make(chan *sarama.ProducerError),
	}
}

func (p *fakeAsyncProducer) AsyncClose()  {}
func (p *fakeAsyncProducer) Close() error { return nil }
func (p *fakeAsyncProducer) Input() chan<- *sarama.ProducerMessage {
	return p.input
}
func (p *fakeAsyncProducer) Successes() <-chan *sarama.ProducerMessage {
	return p.successes
}
func (p *fakeAsyncProducer) Errors() <-chan *sarama.ProducerError {
	return p.errors
}
func (p *fakeAsyncProducer) IsTransactional() bool { return false }
func (p *fakeAsyncProducer) TxnStatus() sarama.ProducerTxnStatusFlag {
	return 0
}
func (p *fakeAsyncProducer) BeginTxn() error  { return nil }
func (p *fakeAsyncProducer) CommitTxn() error { return nil }
func (p *fakeAsyncProducer) AbortTxn() error  { return nil }
func (p *fakeAsyncProducer) AddOffsetsToTxn(offsets map[string][]*sarama.PartitionOffsetMetadata, groupId string) error {
	return nil
}
func (p *fakeAsyncProducer) AddMessageToTxn(msg *sarama.ConsumerMessage, groupId string, metadata *string) error {
	return nil
}

type fakeListener struct {
	addr net.Addr
}

func (l fakeListener) Accept() (net.Conn, error) { return nil, errors.New("closed") }
func (l fakeListener) Close() error              { return nil }
func (l fakeListener) Addr() net.Addr            { return l.addr }

func TestKafkaTopic(t *testing.T) {
	tests := []struct {
		name   string
		prefix string
		topic  string
		want   string
	}{
		{name: "empty prefix", topic: "message-events", want: "message-events"},
		{name: "plain prefix", prefix: "tinode", topic: "message-events", want: "tinode.message-events"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			k := &Kafka{prefix: tt.prefix}
			if got := k.topic(tt.topic); got != tt.want {
				t.Fatalf("topic() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestKafkaSendAndDLQ(t *testing.T) {
	prod := newFakeAsyncProducer()
	k := &Kafka{prod: prod, prefix: "tinode", log: slog.New(slog.NewTextHandler(io.Discard, nil))}

	if err := k.send("message-events", "grp1", []byte("payload")); err != nil {
		t.Fatalf("send returned error: %v", err)
	}
	got := <-prod.input
	if got.Topic != "tinode.message-events" {
		t.Fatalf("topic = %q", got.Topic)
	}
	if key, err := got.Key.Encode(); err != nil || string(key) != "grp1" {
		t.Fatalf("key = %q, err = %v", key, err)
	}
	if value, err := got.Value.Encode(); err != nil || string(value) != "payload" {
		t.Fatalf("value = %q, err = %v", value, err)
	}
	if k.sentCount.Load() != 1 {
		t.Fatalf("sentCount = %d, want 1", k.sentCount.Load())
	}

	k.sendToDLQ("grp1", []byte("bad"), "reason")
	got = <-prod.input
	if got.Topic != "tinode.dlq" {
		t.Fatalf("dlq topic = %q", got.Topic)
	}
}

func TestNewKafkaReturnsProducerError(t *testing.T) {
	tuned := false
	_, err := newKafka(nil, "client", "tinode.", slog.New(slog.NewTextHandler(io.Discard, nil)), func(sc *sarama.Config) {
		tuned = true
		sc.Net.SASL.Enable = true
		sc.Net.SASL.User = "user"
		sc.Net.SASL.Password = "pass"
	})
	if err == nil {
		t.Fatalf("newKafka error = nil, want error")
	}
	if !tuned {
		t.Fatalf("tune function was not called")
	}
}

func TestDefaultAppDeps(t *testing.T) {
	deps := defaultAppDeps()
	if deps.loadConfig == nil || deps.newKafka == nil || deps.listen == nil || deps.serveGRPC == nil || deps.listenAndServe == nil || deps.notify == nil || deps.exit == nil || deps.logWriter == nil {
		t.Fatalf("default dependencies are incomplete: %#v", deps)
	}
}

func TestSCRAMClientBeginAndDone(t *testing.T) {
	client := &XDGSCRAMClient{HashGeneratorFcn: SHA256}
	if err := client.Begin("user", "pass", ""); err != nil {
		t.Fatalf("Begin returned error: %v", err)
	}
	if client.Client == nil || client.ClientConversation == nil {
		t.Fatalf("client was not initialized")
	}
	if client.Done() {
		t.Fatalf("Done = true before conversation completed")
	}
}

func TestGetClientAddrAndLoggingInterceptor(t *testing.T) {
	addr := &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 1234}
	ctx := peer.NewContext(context.Background(), &peer.Peer{Addr: addr})
	if got := getClientAddr(ctx); got != "127.0.0.1:1234" {
		t.Fatalf("getClientAddr = %q", got)
	}
	if got := getClientAddr(context.Background()); got != "unknown" {
		t.Fatalf("getClientAddr without peer = %q", got)
	}

	interceptor := loggingInterceptor(slog.New(slog.NewTextHandler(io.Discard, nil)))
	called := false
	resp, err := interceptor(ctx, "req", &grpc.UnaryServerInfo{FullMethod: "/pbx.Plugin/Find"}, func(ctx context.Context, req interface{}) (interface{}, error) {
		called = true
		return "resp", nil
	})
	if err != nil || resp != "resp" || !called {
		t.Fatalf("interceptor success = (%v, %v, %v)", resp, err, called)
	}
	_, err = interceptor(ctx, "req", &grpc.UnaryServerInfo{FullMethod: "/pbx.Plugin/Find"}, func(ctx context.Context, req interface{}) (interface{}, error) {
		return nil, errors.New("boom")
	})
	if err == nil {
		t.Fatalf("interceptor error = nil, want boom")
	}
}

func TestLogLevel(t *testing.T) {
	tests := map[string]slog.Level{
		"debug":   slog.LevelDebug,
		"warn":    slog.LevelWarn,
		"warning": slog.LevelWarn,
		"error":   slog.LevelError,
		"info":    slog.LevelInfo,
		"":        slog.LevelInfo,
	}
	for input, want := range tests {
		if got := logLevel(input); got != want {
			t.Fatalf("logLevel(%q) = %v, want %v", input, got, want)
		}
	}
}

func TestConfigureKafkaSecurity(t *testing.T) {
	tests := []struct {
		name      string
		mechanism string
		want      sarama.SASLMechanism
		wantSCRAM bool
	}{
		{"plain", "PLAIN", sarama.SASLTypePlaintext, false},
		{"scram sha256", "SCRAM-SHA-256", sarama.SASLTypeSCRAMSHA256, true},
		{"scram sha512", "SCRAM-SHA-512", sarama.SASLTypeSCRAMSHA512, true},
		{"unknown", "UNKNOWN", sarama.SASLTypePlaintext, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sc := sarama.NewConfig()
			configureKafkaSecurity(sc, config.Config{
				KafkaTLSEnable:             true,
				KafkaTLSInsecureSkipVerify: true,
				KafkaSASLEnable:            true,
				KafkaSASLMechanism:         tt.mechanism,
				KafkaSASLUsername:          "user",
				KafkaSASLPassword:          "pass",
			}, slog.New(slog.NewTextHandler(io.Discard, nil)))
			if !sc.Net.TLS.Enable || sc.Net.TLS.Config == nil || !sc.Net.TLS.Config.InsecureSkipVerify {
				t.Fatalf("TLS config mismatch: %#v", sc.Net.TLS)
			}
			if !sc.Net.SASL.Enable || sc.Net.SASL.User != "user" || sc.Net.SASL.Password != "pass" {
				t.Fatalf("SASL config mismatch: %#v", sc.Net.SASL)
			}
			if sc.Net.SASL.Mechanism != tt.want {
				t.Fatalf("mechanism = %s, want %s", sc.Net.SASL.Mechanism, tt.want)
			}
			if (sc.Net.SASL.SCRAMClientGeneratorFunc != nil) != tt.wantSCRAM {
				t.Fatalf("SCRAM generator presence = %v, want %v", sc.Net.SASL.SCRAMClientGeneratorFunc != nil, tt.wantSCRAM)
			}
		})
	}
}

func TestConfigureKafkaSecurityDisabled(t *testing.T) {
	sc := sarama.NewConfig()
	configureKafkaSecurity(sc, config.Config{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if sc.Net.TLS.Enable || sc.Net.SASL.Enable {
		t.Fatalf("security enabled unexpectedly")
	}
}

func TestHealthMux(t *testing.T) {
	k := &Kafka{}
	k.sentCount.Store(5)
	k.ackCount.Store(3)
	k.errorCount.Store(1)
	s := testServer(&fakeKafka{})
	s.fireHoseCount.Store(1)
	s.findCount.Store(2)
	s.accountCount.Store(3)
	s.topicCount.Store(4)
	s.subscriptionCount.Store(5)
	s.messageCount.Store(6)

	mux := newHealthMux(k, s, time.Now().Add(-2*time.Second))
	health := httptest.NewRecorder()
	mux.ServeHTTP(health, httptest.NewRequest(http.MethodGet, "/health", nil))
	if health.Code != http.StatusOK || health.Body.String() != "ok" {
		t.Fatalf("/health = %d %q", health.Code, health.Body.String())
	}

	stats := httptest.NewRecorder()
	mux.ServeHTTP(stats, httptest.NewRequest(http.MethodGet, "/stats", nil))
	if stats.Code != http.StatusOK {
		t.Fatalf("/stats status = %d", stats.Code)
	}
	if got := stats.Header().Get("Content-Type"); got != "application/json" {
		t.Fatalf("content-type = %q", got)
	}
	var body struct {
		Kafka struct {
			Sent    int `json:"sent"`
			Acks    int `json:"acks"`
			Errors  int `json:"errors"`
			Pending int `json:"pending"`
		} `json:"kafka"`
		GRPC struct {
			FirehoseCalls     int `json:"firehose_calls"`
			FindCalls         int `json:"find_calls"`
			AccountCalls      int `json:"account_calls"`
			TopicCalls        int `json:"topic_calls"`
			SubscriptionCalls int `json:"subscription_calls"`
			MessageCalls      int `json:"message_calls"`
		} `json:"grpc"`
	}
	if err := json.Unmarshal(stats.Body.Bytes(), &body); err != nil {
		t.Fatalf("stats json: %v", err)
	}
	if body.Kafka.Sent != 5 || body.Kafka.Acks != 3 || body.Kafka.Errors != 1 || body.Kafka.Pending != 1 {
		t.Fatalf("kafka stats mismatch: %#v", body.Kafka)
	}
	if body.GRPC.FirehoseCalls != 1 || body.GRPC.FindCalls != 2 || body.GRPC.AccountCalls != 3 || body.GRPC.TopicCalls != 4 || body.GRPC.SubscriptionCalls != 5 || body.GRPC.MessageCalls != 6 {
		t.Fatalf("grpc stats mismatch: %#v", body.GRPC)
	}
}

func TestRunAppHappyPath(t *testing.T) {
	listener := fakeListener{addr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 7911}}

	prod := newFakeAsyncProducer()
	var exitCode *int
	var servedGRPC bool
	var servedHTTP bool
	grpcDone := make(chan struct{})
	httpDone := make(chan struct{})
	deps := appDeps{
		loadConfig: func() (config.Config, error) {
			return config.Config{
				ListenAddr:    listener.Addr().String(),
				HealthAddr:    "127.0.0.1:0",
				KafkaBrokers:  []string{"unused:9092"},
				KafkaClientID: "router-test",
				TopicPrefix:   "tinode",
				LogLevel:      "debug",
			}, nil
		},
		newKafka: func(brokers []string, clientID, prefix string, log *slog.Logger, tune func(*sarama.Config)) (*Kafka, error) {
			if len(brokers) != 1 || brokers[0] != "unused:9092" || clientID != "router-test" || prefix != "tinode" {
				t.Fatalf("newKafka args = %#v %s %s", brokers, clientID, prefix)
			}
			sc := sarama.NewConfig()
			tune(sc)
			return &Kafka{prod: prod, prefix: prefix, log: log}, nil
		},
		listen: func(network, addr string) (net.Listener, error) {
			if network != "tcp" || addr != listener.Addr().String() {
				t.Fatalf("listen args = %s %s", network, addr)
			}
			return listener, nil
		},
		serveGRPC: func(s *grpc.Server, l net.Listener) error {
			servedGRPC = true
			close(grpcDone)
			return nil
		},
		listenAndServe: func(addr string, handler http.Handler) error {
			servedHTTP = true
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/health", nil))
			if rec.Code != http.StatusOK {
				t.Fatalf("health status = %d", rec.Code)
			}
			close(httpDone)
			return http.ErrServerClosed
		},
		notify: func(sig chan<- os.Signal, signals ...os.Signal) {
			<-grpcDone
			<-httpDone
			sig <- syscall.SIGTERM
		},
		exit: func(code int) {
			exitCode = &code
		},
		logWriter: io.Discard,
	}

	if err := runApp(deps); err != nil {
		t.Fatalf("runApp returned error: %v", err)
	}
	if !servedGRPC || !servedHTTP {
		t.Fatalf("served grpc/http = %v/%v", servedGRPC, servedHTTP)
	}
	if exitCode != nil {
		t.Fatalf("exit called with %d", *exitCode)
	}
}

func TestRunAppReturnsStartupErrors(t *testing.T) {
	tests := []struct {
		name string
		deps appDeps
	}{
		{
			name: "config",
			deps: appDeps{loadConfig: func() (config.Config, error) {
				return config.Config{}, errors.New("bad config")
			}},
		},
		{
			name: "kafka",
			deps: appDeps{
				loadConfig: func() (config.Config, error) {
					return config.Config{KafkaBrokers: []string{"unused"}, KafkaClientID: "id"}, nil
				},
				newKafka: func([]string, string, string, *slog.Logger, func(*sarama.Config)) (*Kafka, error) {
					return nil, errors.New("kafka down")
				},
				logWriter: io.Discard,
			},
		},
		{
			name: "listen",
			deps: appDeps{
				loadConfig: func() (config.Config, error) {
					return config.Config{KafkaBrokers: []string{"unused"}, KafkaClientID: "id"}, nil
				},
				newKafka: func([]string, string, string, *slog.Logger, func(*sarama.Config)) (*Kafka, error) {
					return &Kafka{prod: newFakeAsyncProducer(), log: slog.New(slog.NewTextHandler(io.Discard, nil))}, nil
				},
				listen: func(string, string) (net.Listener, error) {
					return nil, errors.New("bind failed")
				},
				logWriter: io.Discard,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := runApp(tt.deps); err == nil {
				t.Fatalf("runApp error = nil, want error")
			}
		})
	}
}

func TestClassifyMessage(t *testing.T) {
	s := testServer(&fakeKafka{})
	tests := []struct {
		name      string
		msg       *pbx.ClientMsg
		wantType  string
		wantTopic string
		wantKey   string
	}{
		{"hi", &pbx.ClientMsg{Message: &pbx.ClientMsg_Hi{Hi: &pbx.ClientHi{}}}, "hi", topicFirehoseHandshake, "usr1"},
		{"acc uses target user", &pbx.ClientMsg{Message: &pbx.ClientMsg_Acc{Acc: &pbx.ClientAcc{UserId: "usr2"}}}, "acc", topicFirehoseAccountMgmt, "usr2"},
		{"acc falls back to session user", &pbx.ClientMsg{Message: &pbx.ClientMsg_Acc{Acc: &pbx.ClientAcc{}}}, "acc", topicFirehoseAccountMgmt, "usr1"},
		{"login", &pbx.ClientMsg{Message: &pbx.ClientMsg_Login{Login: &pbx.ClientLogin{}}}, "login", topicFirehoseAuth, "usr1"},
		{"sub", &pbx.ClientMsg{Message: &pbx.ClientMsg_Sub{Sub: &pbx.ClientSub{Topic: "grp1"}}}, "sub", topicFirehoseSubscriptions, "grp1"},
		{"leave", &pbx.ClientMsg{Message: &pbx.ClientMsg_Leave{Leave: &pbx.ClientLeave{Topic: "grp1"}}}, "leave", topicFirehoseSubscriptions, "grp1"},
		{"pub", &pbx.ClientMsg{Message: &pbx.ClientMsg_Pub{Pub: &pbx.ClientPub{Topic: "grp1"}}}, "pub", topicFirehoseMessages, "grp1"},
		{"get", &pbx.ClientMsg{Message: &pbx.ClientMsg_Get{Get: &pbx.ClientGet{Topic: "grp1"}}}, "get", topicFirehoseQueries, "usr1"},
		{"set", &pbx.ClientMsg{Message: &pbx.ClientMsg_Set{Set: &pbx.ClientSet{Topic: "grp1"}}}, "set", topicFirehoseUpdates, "grp1"},
		{"del", &pbx.ClientMsg{Message: &pbx.ClientMsg_Del{Del: &pbx.ClientDel{Topic: "grp1"}}}, "del", topicFirehoseDeletions, "grp1"},
		{"note", &pbx.ClientMsg{Message: &pbx.ClientMsg_Note{Note: &pbx.ClientNote{Topic: "grp1"}}}, "note", topicFirehoseNotifications, "grp1"},
		{"unknown", &pbx.ClientMsg{}, "unknown", topicDLQ, "usr1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotType, gotTopic, gotKey := s.classifyMessage(tt.msg, "usr1")
			if gotType != tt.wantType || gotTopic != tt.wantTopic || gotKey != tt.wantKey {
				t.Fatalf("classifyMessage() = (%q, %q, %q), want (%q, %q, %q)", gotType, gotTopic, gotKey, tt.wantType, tt.wantTopic, tt.wantKey)
			}
		})
	}
}

func TestRedactSecrets(t *testing.T) {
	acc := &pbx.ClientMsg{Message: &pbx.ClientMsg_Acc{Acc: &pbx.ClientAcc{Secret: []byte("secret"), TmpSecret: []byte("tmp")}}}
	redactSecrets(acc)
	if got := acc.GetAcc().GetSecret(); got != nil {
		t.Fatalf("acc secret = %q, want nil", got)
	}
	if got := acc.GetAcc().GetTmpSecret(); got != nil {
		t.Fatalf("acc tmp secret = %q, want nil", got)
	}

	login := &pbx.ClientMsg{Message: &pbx.ClientMsg_Login{Login: &pbx.ClientLogin{Secret: []byte("secret")}}}
	redactSecrets(login)
	if got := login.GetLogin().GetSecret(); got != nil {
		t.Fatalf("login secret = %q, want nil", got)
	}

	redactSecrets(nil)
}

func TestFireHoseSendsSanitizedPayloadAndDLQOnError(t *testing.T) {
	k := &fakeKafka{err: errors.New("kafka down")}
	s := testServer(k)
	req := &pbx.ClientReq{
		Sess: &pbx.Session{UserId: "usr1", SessionId: "sess1"},
		Msg:  &pbx.ClientMsg{Message: &pbx.ClientMsg_Login{Login: &pbx.ClientLogin{Id: "1", Secret: []byte("secret")}}},
	}

	resp, err := s.FireHose(context.Background(), req)
	if err != nil {
		t.Fatalf("FireHose returned error: %v", err)
	}
	if resp.GetStatus() != pbx.RespCode_CONTINUE {
		t.Fatalf("status = %v, want CONTINUE", resp.GetStatus())
	}
	if len(k.sent) != 1 {
		t.Fatalf("sent = %d, want 1", len(k.sent))
	}
	if k.sent[0].topic != topicFirehoseAuth || k.sent[0].key != "usr1" {
		t.Fatalf("sent route = %s/%s", k.sent[0].topic, k.sent[0].key)
	}
	var got pbx.ClientReq
	if err := proto.Unmarshal(k.sent[0].value, &got); err != nil {
		t.Fatalf("unmarshal sent payload: %v", err)
	}
	if got.GetMsg().GetLogin().GetSecret() != nil {
		t.Fatalf("login secret was not redacted")
	}
	if len(k.dlq) != 1 || k.dlq[0].key != "usr1" {
		t.Fatalf("dlq = %#v, want one message keyed by usr1", k.dlq)
	}
}

func TestFireHoseNilInputsContinueWithoutSend(t *testing.T) {
	k := &fakeKafka{}
	s := testServer(k)

	for _, req := range []*pbx.ClientReq{nil, {Sess: &pbx.Session{UserId: "usr1"}}} {
		resp, err := s.FireHose(context.Background(), req)
		if err != nil {
			t.Fatalf("FireHose returned error: %v", err)
		}
		if resp.GetStatus() != pbx.RespCode_CONTINUE {
			t.Fatalf("status = %v, want CONTINUE", resp.GetStatus())
		}
	}
	if len(k.sent) != 0 {
		t.Fatalf("sent = %d, want 0", len(k.sent))
	}
}

func TestPluginUnarySendsExpectedTopics(t *testing.T) {
	tests := []struct {
		name      string
		call      func(*pluginServer) error
		wantTopic string
		wantKey   string
		wantDLQ   bool
	}{
		{
			name: "find",
			call: func(s *pluginServer) error {
				_, err := s.Find(context.Background(), &pbx.SearchQuery{UserId: "usr1", Query: "alice"})
				return err
			},
			wantTopic: topicSearchQueries, wantKey: "usr1",
		},
		{
			name: "account",
			call: func(s *pluginServer) error {
				_, err := s.Account(context.Background(), &pbx.AccountEvent{UserId: "usr1", Action: pbx.Crud_CREATE})
				return err
			},
			wantTopic: topicAccountEvents, wantKey: "usr1", wantDLQ: true,
		},
		{
			name: "topic",
			call: func(s *pluginServer) error {
				_, err := s.Topic(context.Background(), &pbx.TopicEvent{Name: "grp1", Action: pbx.Crud_UPDATE})
				return err
			},
			wantTopic: topicTopicEvents, wantKey: "grp1", wantDLQ: true,
		},
		{
			name: "subscription",
			call: func(s *pluginServer) error {
				_, err := s.Subscription(context.Background(), &pbx.SubscriptionEvent{Topic: "grp1", UserId: "usr1", Action: pbx.Crud_UPDATE})
				return err
			},
			wantTopic: topicSubscriptionEvents, wantKey: "grp1", wantDLQ: true,
		},
		{
			name: "message",
			call: func(s *pluginServer) error {
				_, err := s.Message(context.Background(), &pbx.MessageEvent{Action: pbx.Crud_CREATE, Msg: &pbx.ServerData{Topic: "grp1", FromUserId: "usr1"}})
				return err
			},
			wantTopic: topicMessageEvents, wantKey: "grp1", wantDLQ: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			k := &fakeKafka{err: errors.New("send failed")}
			s := testServer(k)
			if err := tt.call(s); err != nil {
				t.Fatalf("call returned error: %v", err)
			}
			if len(k.sent) != 1 {
				t.Fatalf("sent = %d, want 1", len(k.sent))
			}
			if k.sent[0].topic != tt.wantTopic || k.sent[0].key != tt.wantKey {
				t.Fatalf("sent route = %s/%s, want %s/%s", k.sent[0].topic, k.sent[0].key, tt.wantTopic, tt.wantKey)
			}
			if tt.wantDLQ && len(k.dlq) != 1 {
				t.Fatalf("dlq = %d, want 1", len(k.dlq))
			}
		})
	}
}

func TestPluginUnaryNilInputs(t *testing.T) {
	s := testServer(&fakeKafka{})
	if _, err := s.Account(context.Background(), nil); err == nil {
		t.Fatalf("Account(nil) error = nil, want error")
	}
	if _, err := s.Topic(context.Background(), nil); err == nil {
		t.Fatalf("Topic(nil) error = nil, want error")
	}
	if _, err := s.Subscription(context.Background(), nil); err == nil {
		t.Fatalf("Subscription(nil) error = nil, want error")
	}
	if _, err := s.Message(context.Background(), nil); err == nil {
		t.Fatalf("Message(nil) error = nil, want error")
	}
	if resp, err := s.Find(context.Background(), nil); err != nil || resp.GetStatus() != pbx.RespCode_CONTINUE {
		t.Fatalf("Find(nil) = (%v, %v), want CONTINUE nil", resp, err)
	}
}

func TestSCRAMClientStep(t *testing.T) {
	client := &XDGSCRAMClient{HashGeneratorFcn: SHA256}
	if err := client.Begin("user", "pass", ""); err != nil {
		t.Fatalf("Begin returned error: %v", err)
	}

	resp, err := client.Step("test")
	if err != nil {
		t.Fatalf("Step returned error: %v", err)
	}
	if resp == "" {
		t.Fatalf("Step returned empty response")
	}
}

func TestKafkaSendTimeout(t *testing.T) {
	blocked := make(chan struct{})
	prod := &fakeAsyncProducer{
		input:     make(chan *sarama.ProducerMessage),
		successes: make(chan *sarama.ProducerMessage),
		errors:    make(chan *sarama.ProducerError),
	}

	k := &Kafka{prod: prod, prefix: "test", log: slog.New(slog.NewTextHandler(io.Discard, nil))}

	go func() {
		close(blocked)
		time.Sleep(time.Second)
	}()

	<-blocked
	err := k.send("topic", "key", []byte("value"))
	if err == nil {
		t.Fatalf("send should timeout, got nil error")
	}
}

func TestKafkaProducerChannelsClosed(t *testing.T) {
	prod := newFakeAsyncProducer()
	_ = &Kafka{prod: prod, prefix: "test", log: slog.New(slog.NewTextHandler(io.Discard, nil))}

	go func() {
		time.Sleep(10 * time.Millisecond)
		close(prod.successes)
		close(prod.errors)
	}()

	time.Sleep(50 * time.Millisecond)
}

func TestDefaultAppDepsIndividualFunctions(t *testing.T) {
	deps := defaultAppDeps()

	if _, err := deps.loadConfig(); err != nil {
		t.Logf("loadConfig called (expected to fail without .env): %v", err)
	}

	listener, err := deps.listen("tcp", ":0")
	if err != nil {
		t.Fatalf("listen failed: %v", err)
	}
	listener.Close()

	srv := grpc.NewServer()
	defer srv.GracefulStop()

	listener, _ = net.Listen("tcp", ":0")
	go func() {
		time.Sleep(10 * time.Millisecond)
		srv.GracefulStop()
	}()

	err = deps.serveGRPC(srv, listener)
	if err != nil && err != grpc.ErrServerStopped {
		t.Logf("serveGRPC returned: %v", err)
	}
	listener.Close()

	ch := make(chan os.Signal, 1)
	go func() {
		time.Sleep(10 * time.Millisecond)
		ch <- syscall.SIGTERM
	}()

	deps.notify(ch, syscall.SIGTERM)
	sig := <-ch
	if sig != syscall.SIGTERM {
		t.Fatalf("notify didn't work, got signal %v", sig)
	}

	if deps.logWriter == nil {
		t.Fatalf("logWriter is nil")
	}
}

func TestConfigureKafkaSecurityTLS(t *testing.T) {
	sc := sarama.NewConfig()
	configureKafkaSecurity(sc, config.Config{
		KafkaTLSEnable:             true,
		KafkaTLSInsecureSkipVerify: false,
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))

	if !sc.Net.TLS.Enable {
		t.Fatalf("TLS not enabled")
	}
	if sc.Net.TLS.Config == nil {
		t.Fatalf("TLS config is nil")
	}
	if sc.Net.TLS.Config.InsecureSkipVerify {
		t.Fatalf("InsecureSkipVerify should be false")
	}
}

func TestPluginFindWithoutUserId(t *testing.T) {
	k := &fakeKafka{}
	s := testServer(k)
	resp, err := s.Find(context.Background(), &pbx.SearchQuery{})
	if err != nil {
		t.Fatalf("Find returned error: %v", err)
	}
	if resp.GetStatus() != pbx.RespCode_CONTINUE {
		t.Fatalf("status = %v, want CONTINUE", resp.GetStatus())
	}
}

func TestPluginAccountSendsToDLQOnError(t *testing.T) {
	k := &fakeKafka{err: errors.New("kafka down")}
	s := testServer(k)
	_, err := s.Account(context.Background(), &pbx.AccountEvent{UserId: "usr1", Action: pbx.Crud_CREATE})
	if err != nil {
		t.Fatalf("Account returned error: %v", err)
	}
	if len(k.dlq) != 1 {
		t.Fatalf("dlq = %d, want 1", len(k.dlq))
	}
}

func TestPluginTopicSendsToDLQOnError(t *testing.T) {
	k := &fakeKafka{err: errors.New("kafka down")}
	s := testServer(k)
	_, err := s.Topic(context.Background(), &pbx.TopicEvent{Name: "grp1", Action: pbx.Crud_UPDATE})
	if err != nil {
		t.Fatalf("Topic returned error: %v", err)
	}
	if len(k.dlq) != 1 {
		t.Fatalf("dlq = %d, want 1", len(k.dlq))
	}
}

func TestPluginSubscriptionSendsToDLQOnError(t *testing.T) {
	k := &fakeKafka{err: errors.New("kafka down")}
	s := testServer(k)
	_, err := s.Subscription(context.Background(), &pbx.SubscriptionEvent{Topic: "grp1", UserId: "usr1", Action: pbx.Crud_UPDATE})
	if err != nil {
		t.Fatalf("Subscription returned error: %v", err)
	}
	if len(k.dlq) != 1 {
		t.Fatalf("dlq = %d, want 1", len(k.dlq))
	}
}

func TestPluginMessageSendsToDLQOnError(t *testing.T) {
	k := &fakeKafka{err: errors.New("kafka down")}
	s := testServer(k)
	_, err := s.Message(context.Background(), &pbx.MessageEvent{Action: pbx.Crud_CREATE, Msg: &pbx.ServerData{Topic: "grp1", FromUserId: "usr1"}})
	if err != nil {
		t.Fatalf("Message returned error: %v", err)
	}
	if len(k.dlq) != 1 {
		t.Fatalf("dlq = %d, want 1", len(k.dlq))
	}
}

func TestKafkaPrefixTrimming(t *testing.T) {
	prod := newFakeAsyncProducer()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	tests := []struct {
		input string
		want  string
	}{
		{"tinode.", "tinode"},
		{"tinode", "tinode"},
		{"test.", "test"},
		{"", ""},
		{"test..", "test."},
	}

	for _, tt := range tests {
		t.Run("prefix_"+tt.input, func(t *testing.T) {
			k := &Kafka{
				prod:   prod,
				prefix: strings.TrimSuffix(tt.input, "."),
				log:    logger,
			}
			if k.prefix != tt.want {
				t.Fatalf("prefix = %q, want %q", k.prefix, tt.want)
			}
		})
	}
}

func TestKafkaInitializesWithCorrectConfig(t *testing.T) {
	prod := newFakeAsyncProducer()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	k := &Kafka{
		prod:   prod,
		prefix: "tinode",
		log:    logger,
	}

	if k.sentCount.Load() != 0 {
		t.Fatalf("sentCount = %d, want 0", k.sentCount.Load())
	}
	if k.ackCount.Load() != 0 {
		t.Fatalf("ackCount = %d, want 0", k.ackCount.Load())
	}
	if k.errorCount.Load() != 0 {
		t.Fatalf("errorCount = %d, want 0", k.errorCount.Load())
	}
	if k.prefix != "tinode" {
		t.Fatalf("prefix = %q, want tinode", k.prefix)
	}
}

func TestKafkaSendToMultipleTopics(t *testing.T) {
	prod := newFakeAsyncProducer()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	k := &Kafka{
		prod:   prod,
		prefix: "tinode",
		log:    logger,
	}

	topics := []string{"account-events", "message-events", "topic-events"}
	for _, topic := range topics {
		expected := "tinode." + topic
		got := k.topic(topic)
		if got != expected {
			t.Fatalf("topic(%s) = %q, want %q", topic, got, expected)
		}
	}

	if k.topic("dlq") != "tinode.dlq" {
		t.Fatalf("topic(dlq) failed")
	}
}

func TestKafkaSendDLQWithReason(t *testing.T) {
	prod := newFakeAsyncProducer()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	k := &Kafka{
		prod:   prod,
		prefix: "tinode",
		log:    logger,
	}

	k.sendToDLQ("key1", []byte("failed_data"), "connection timeout")

	if k.sentCount.Load() != 1 {
		t.Fatalf("sentCount = %d, want 1", k.sentCount.Load())
	}

	msg := <-prod.input
	if msg.Topic != "tinode.dlq" {
		t.Fatalf("topic = %q, want tinode.dlq", msg.Topic)
	}
}

func TestKafkaTopicWithoutPrefix(t *testing.T) {
	prod := newFakeAsyncProducer()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	k := &Kafka{
		prod:   prod,
		prefix: "",
		log:    logger,
	}

	fullTopic := k.topic("message-events")
	if fullTopic != "message-events" {
		t.Fatalf("topic = %q, want message-events", fullTopic)
	}
}

func TestKafkaTracksStatistics(t *testing.T) {
	prod := newFakeAsyncProducer()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	k := &Kafka{
		prod:   prod,
		prefix: "test",
		log:    logger,
	}

	k.sentCount.Store(100)
	k.ackCount.Store(80)
	k.errorCount.Store(5)

	if k.sentCount.Load() != 100 {
		t.Fatalf("sentCount = %d, want 100", k.sentCount.Load())
	}
	if k.ackCount.Load() != 80 {
		t.Fatalf("ackCount = %d, want 80", k.ackCount.Load())
	}
	if k.errorCount.Load() != 5 {
		t.Fatalf("errorCount = %d, want 5", k.errorCount.Load())
	}

	pending := k.sentCount.Load() - k.ackCount.Load() - k.errorCount.Load()
	if pending != 15 {
		t.Fatalf("pending = %d, want 15", pending)
	}
}
