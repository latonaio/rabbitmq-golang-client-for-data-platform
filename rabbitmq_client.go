package rabbitmq_client

import (
	"github.com/latonaio/rabbitmq-golang-client-for-data-platform/internal/rabbitmq_client/client"
	"github.com/latonaio/rabbitmq-golang-client-for-data-platform/internal/rabbitmq_client/types"
)

// RabbitmqConfig は、RabbitMQ への接続情報を保持するインターフェースです。
type RabbitmqConfig = types.RabbitmqConfig

// RabbitmqClient は、RabbitMQ への接続を管理する構造体です。
type RabbitmqClient = client.RabbitmqClient

// RabbitmqMessage は、RabbitMQ から受信したメッセージを保持するインターフェースです。
type RabbitmqMessage = types.RabbitmqMessage

// NewRabbitmqClient は、RabbitMQ への接続を開始します。
func NewRabbitmqClientWithConfig(config RabbitmqConfig) (*RabbitmqClient, error) {
	return client.NewRabbitmqClientWithConfig(config)
}
func NewRabbitmqClient(url string, queueFrom string, queueFromResponse string, queueTo []string, prefetchCount int) (*RabbitmqClient, error) {
	return client.NewRabbitmqClient(url, queueFrom, queueFromResponse, queueTo, prefetchCount)
}
