package config

import (
	"fmt"
	"strings"

	"github.com/joho/godotenv"
	"github.com/kelseyhightower/envconfig"
)

type Config struct {
	// Сетевые адреса
	ListenAddr string `envconfig:"LISTEN_ADDR" default:":7911"`
	HealthAddr string `envconfig:"HEALTH_ADDR" default:":8090"`

	// Kafka
	KafkaBrokers  []string `envconfig:"KAFKA_BROKERS" default:"localhost:9092"` // CSV
	KafkaClientID string   `envconfig:"KAFKA_CLIENT_ID" default:"tinode-listener"`
	TopicPrefix   string   `envconfig:"TOPIC_PREFIX" default:"tinode"`

	// Логирование
	LogLevel string `envconfig:"LOG_LEVEL" default:"info"` // debug|info|warn|error

	// Безопасность Kafka
	KafkaSASLEnable            bool   `envconfig:"KAFKA_SASL_ENABLE" default:"false"`
	KafkaSASLMechanism         string `envconfig:"KAFKA_SASL_MECHANISM" default:"PLAIN"` // PLAIN|SCRAM-SHA-256|SCRAM-SHA-512
	KafkaSASLUsername          string `envconfig:"KAFKA_SASL_USERNAME"`
	KafkaSASLPassword          string `envconfig:"KAFKA_SASL_PASSWORD"`
	KafkaTLSEnable             bool   `envconfig:"KAFKA_TLS_ENABLE" default:"false"`
	KafkaTLSInsecureSkipVerify bool   `envconfig:"KAFKA_TLS_INSECURE_SKIP_VERIFY" default:"false"`
}

func Load() (Config, error) {
	// .env опционален; если файла нет — игнорируем ошибку.
	_ = godotenv.Load()

	var c Config
	if err := envconfig.Process("", &c); err != nil {
		return c, err
	}

	// Нормализуем префикс: без завершающей точки
	c.TopicPrefix = strings.TrimSuffix(c.TopicPrefix, ".")

	// Валидация базовых полей
	if len(c.KafkaBrokers) == 0 {
		return c, fmt.Errorf("KAFKA_BROKERS is empty")
	}
	return c, nil
}
