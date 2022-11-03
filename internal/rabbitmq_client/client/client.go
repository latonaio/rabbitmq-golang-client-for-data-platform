package client

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/latonaio/rabbitmq-golang-client-for-data-platform/internal/rabbitmq_client/channel_manager"
	"github.com/latonaio/rabbitmq-golang-client-for-data-platform/internal/rabbitmq_client/types"
	"github.com/streadway/amqp"
)

const defaultPrefetchCount = 32

type RabbitmqClient struct {
	connection             *amqp.Connection
	url                    string
	sessions               *sync.Map
	queueFrom              string
	queueFromResponse      string
	isClosed               bool
	chManager              *channel_manager.RabbitmqChannelManager
	sessionChManager       *channel_manager.RabbitmqChannelManager
	responseIteratorCtx    context.Context
	responseIteratorCancel func()
}

func NewRabbitmqClient(url, queueFrom, queueFromResponse string, queueTo []string, prefetchCount int) (*RabbitmqClient, error) {
	// 接続
	conn, err := connect(url)
	if err != nil {
		return nil, err
	}

	// 仮作成
	responseIteratorCtx, responseIteratorCancel := context.WithCancel(context.Background())
	client := &RabbitmqClient{
		connection:             conn,
		url:                    url,
		sessions:               &sync.Map{},
		queueFrom:              queueFrom,
		queueFromResponse:      queueFromResponse,
		responseIteratorCtx:    responseIteratorCtx,
		responseIteratorCancel: responseIteratorCancel,
	}

	// チャンネル作成
	// デフォルトチャンネル
	if prefetchCount <= 0 {
		prefetchCount = defaultPrefetchCount
	}
	chManager, err := channel_manager.CreateChannelManager(client, queueFrom, prefetchCount)
	if err != nil {
		return nil, err
	}
	client.chManager = chManager

	// リクエスト・レスポンス専用チャンネル
	if queueFromResponse != "" {
		sessionChManager, err := channel_manager.CreateChannelManager(client, queueFromResponse, 0)
		if err != nil {
			return nil, err
		}
		client.sessionChManager = sessionChManager
	}

	// queue の名前について重複を除外する
	sendQueues := make(map[string]struct{})
	queues := make(map[string]struct{})
	if queueFrom != "" {
		queues[queueFrom] = struct{}{}
	}
	if queueFromResponse != "" {
		queues[queueFromResponse] = struct{}{}
	}
	for _, queue := range queueTo {
		sendQueues[queue] = struct{}{}
		queues[queue] = struct{}{}
	}

	for queue := range queues {
		if ok := chManager.IsQueueExist(queue); !ok {
			return nil, fmt.Errorf("queue does not exist: %v", queue)
		}
	}

	// 切断時の再接続用ルーチン
	go client.checkConnection()
	// レスポンス受信用ループ
	go client.iterateResponses()

	return client, nil
}

func NewRabbitmqClientWithConfig(config types.RabbitmqConfig) (*RabbitmqClient, error) {
	return NewRabbitmqClient(
		config.URL(),
		config.QueueFrom(),
		config.SessionResponseQueue(),
		config.QueueTo(),
		config.PrefetchCount(),
	)
}

func connect(url string) (*amqp.Connection, error) {
	// 接続
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, fmt.Errorf("connection with rabbitmq error: %w", err)
	}

	return conn, nil
}

func (r *RabbitmqClient) checkConnection() {
	for {
		<-r.connection.NotifyClose(make(chan *amqp.Error))

		log.Println("[rabbitmqClient] disconnected")

		// 切断時
		for {
			if r.isClosed {
				return
			}

			// 5 秒後に再接続
			time.Sleep(5 * time.Second)

			conn, err := connect(r.url)
			if err != nil {
				continue
			}
			r.connection = conn

			// デフォルトチャンネル
			if err := r.chManager.OnReconnect(); err != nil {
				continue
			}
			// リクエスト・レスポンス専用チャンネル
			if r.sessionChManager != nil {
				if err := r.sessionChManager.OnReconnect(); err != nil {
					continue
				}
			}

			log.Println("[rabbitmqClient] reconnected")
			break
		}
	}
}

func (r *RabbitmqClient) iterateResponses() {
	if r.sessionChManager == nil {
		return
	}

	iter, err := r.sessionChManager.Iterator()
	if err != nil {
		log.Printf("[rabbitmqClient] failed to iterate response: %v", err)
		return
	}

	for {
		select {
		case <-r.responseIteratorCtx.Done():
			return

		case msg := <-iter:
			corrID := msg.CorrelationID()
			if corrID == "" {
				log.Println("[rabbitmqClient] [WARNING] received message with empty correlationID")
				msg.Fail()
				continue
			}

			var msgCh chan<- types.RabbitmqMessage
			if msgChUntyped, ok := r.sessions.Load(corrID); !ok {
				log.Printf("[rabbitmqClient] [WARNING] received message with unknown correlationID: %v", corrID)
				msg.Fail()
				continue
			} else {
				msgCh = msgChUntyped.(chan types.RabbitmqMessage)
			}

			msgCh <- msg
		}
	}
}

func (r *RabbitmqClient) CreateChannel() (*amqp.Channel, error) {
	return r.connection.Channel()
}

func (r *RabbitmqClient) Stop() error {
	// chManager (のみ) からの受信を停止する
	return r.chManager.Stop()
}

func (r *RabbitmqClient) Close() error {
	if err := r.chManager.Close(); err != nil {
		return fmt.Errorf("close chManager error: %w", err)
	}
	if r.sessionChManager != nil {
		r.responseIteratorCancel()
		if err := r.sessionChManager.Close(); err != nil {
			return fmt.Errorf("close responseChManager error: %w", err)
		}
	}

	// 実際の接続終了よりも先に接続終了フラグを立てる
	r.isClosed = true

	if err := r.connection.Close(); err != nil {
		return fmt.Errorf("close connection error: %w", err)
	}
	return nil
}

func (r *RabbitmqClient) Iterator() (<-chan types.RabbitmqMessage, error) {
	return r.chManager.Iterator()
}

func (r *RabbitmqClient) SessionKeepRequest(ctx context.Context, sendQueue string, payload interface{}) (types.RabbitmqMessage, error) {
	if ctx == nil {
		ctx, _ = context.WithTimeout(context.Background(), 30*time.Second)
	}
	messageID := uuid.New().String()
	ch := make(chan types.RabbitmqMessage)
	r.sessions.Store(messageID, ch)
	defer close(ch)
	defer r.sessions.Delete(messageID)

	if err := r.sessionChManager.Send(sendQueue, payload, messageID, "", r.sessionChManager.QueueFrom()); err != nil {
		return nil, fmt.Errorf("failed to publish a message: %w", err)
	}

	select {
	case msg := <-ch:
		return msg, nil

	case <-ctx.Done():
		return nil, errors.New("request canceled")
	}
}

func (r *RabbitmqClient) Send(sendQueue string, payload map[string]interface{}) error {
	return r.chManager.Send(sendQueue, payload, "", "", "")
}
