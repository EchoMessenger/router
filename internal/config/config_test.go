package config

import (
	"reflect"
	"testing"
)

func TestLoadDefaultsAndEnvironment(t *testing.T) {
	t.Setenv("LISTEN_ADDR", ":6000")
	t.Setenv("HEALTH_ADDR", ":7000")
	t.Setenv("KAFKA_BROKERS", "k1:9092,k2:9092")
	t.Setenv("KAFKA_CLIENT_ID", "router-test")
	t.Setenv("TOPIC_PREFIX", "tinode.")
	t.Setenv("LOG_LEVEL", "debug")
	t.Setenv("KAFKA_SASL_ENABLE", "true")
	t.Setenv("KAFKA_SASL_USERNAME", "user")
	t.Setenv("KAFKA_SASL_PASSWORD", "pass")
	t.Setenv("KAFKA_TLS_ENABLE", "true")
	t.Setenv("KAFKA_TLS_INSECURE_SKIP_VERIFY", "true")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	if cfg.ListenAddr != ":6000" || cfg.HealthAddr != ":7000" || cfg.KafkaClientID != "router-test" || cfg.TopicPrefix != "tinode" || cfg.LogLevel != "debug" {
		t.Fatalf("unexpected basic cfg: %#v", cfg)
	}
	if !reflect.DeepEqual(cfg.KafkaBrokers, []string{"k1:9092", "k2:9092"}) {
		t.Fatalf("KafkaBrokers = %#v", cfg.KafkaBrokers)
	}
	if !cfg.KafkaSASLEnable || cfg.KafkaSASLUsername != "user" || cfg.KafkaSASLPassword != "pass" {
		t.Fatalf("unexpected SASL cfg: %#v", cfg)
	}
	if !cfg.KafkaTLSEnable || !cfg.KafkaTLSInsecureSkipVerify {
		t.Fatalf("unexpected TLS cfg: %#v", cfg)
	}
}

func TestLoadRejectsSASLWithoutCredentials(t *testing.T) {
	t.Setenv("KAFKA_SASL_ENABLE", "true")
	t.Setenv("KAFKA_SASL_USERNAME", "")
	t.Setenv("KAFKA_SASL_PASSWORD", "")

	if _, err := Load(); err == nil {
		t.Fatalf("Load error = nil, want missing SASL credentials error")
	}
}
