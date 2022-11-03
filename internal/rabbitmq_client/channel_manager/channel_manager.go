package channel_manager

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"

	"github.com/latonaio/rabbitmq-golang-client-for-data-platform/internal/rabbitmq_client/message"
	"github.com/latonaio/rabbitmq-golang-client-for-data-platform/internal/rabbitmq_client/types"
	"github.com/streadway/amqp"
)

const iteratorTag = "tag"

type rabbitmqClient interface {
	CreateChannel() (*amqp.Channel, error)
}

type RabbitmqChannelManager struct {
	client        rabbitmqClient
	channel       *amqp.Channel
	queueFrom     string
	prefetchCount int
	iteratorCh    chan types.RabbitmqMessage
	cancel        context.CancelFunc
}

func CreateChannelManager(r rabbitmqClient, queueFrom string, prefetchCount int) (*RabbitmqChannelManager, error) {
	manager := &RabbitmqChannelManager{
		client:        r,
		queueFrom:     queueFrom,
		prefetchCount: prefetchCount,
	}
	if err := manager.init(); err != nil {
		return nil, err
	}

	return manager, nil
}

func (m *RabbitmqChannelManager) init() error {
	ch, err := m.client.CreateChannel()
	if err != nil {
		return fmt.Errorf("failed to create channel: %w", err)
	}
	m.channel = ch

	if err := ch.Qos(m.prefetchCount, 0, false); err != nil {
		return fmt.Errorf("failed to set channel qos: %w", err)
	}

	return nil
}

func (m *RabbitmqChannelManager) stop() error {
	var err error = nil
	if perr := m.channel.Cancel(iteratorTag, false); perr != nil {
		err = perr
	}

	if m.cancel != nil {
		m.cancel()
		m.cancel = nil
	}

	return err
}

func (m *RabbitmqChannelManager) Stop() error {
	if m.iteratorCh == nil {
		return nil
	}

	if err := m.stop(); err != nil {
		return err
	}

	close(m.iteratorCh)
	m.iteratorCh = nil

	return nil
}

func (m *RabbitmqChannelManager) Iterator() (<-chan types.RabbitmqMessage, error) {
	if m.cancel != nil {
		return nil, errors.New("already iterating")
	}

	if m.iteratorCh == nil {
		m.iteratorCh = make(chan types.RabbitmqMessage)
	}

	ctx, cancel := context.WithCancel(context.Background())
	m.cancel = cancel

	// queue からメッセージの受け取り
	msgs, err := m.channel.Consume(
		m.queueFrom, // queue
		iteratorTag, // consumer
		false,       // auto-ack
		false,       // exclusive
		false,       // no-local
		false,       // no-wait
		nil,         // args
	)
	if err != nil {
		// consume に失敗したら受信を Stop() する
		m.Stop()
		return nil, fmt.Errorf("failed to consume: %w", err)
	}

	go func() {
		for {
			select {
			case d, ok := <-msgs:
				if !ok {
					continue
				}

				msg, err := message.NewRabbitmqMessage(d, m.queueFrom, m)
				if err != nil {
					log.Printf("[RabbitmqClient] failed to parse as json: %v", err)
				}

				m.iteratorCh <- msg
			case <-ctx.Done():
				return
			}
		}
	}()

	return m.iteratorCh, nil
}

func (m *RabbitmqChannelManager) OnReconnect() error {
	if err := m.init(); err != nil {
		return err
	}

	if m.cancel == nil {
		return nil
	}

	// 受信中だった場合、再購読
	if err := m.stop(); err != nil {
		log.Printf("[RabbitmqChannelManager] failed to stop on reconnecting: %v", err)
	}
	if _, err := m.Iterator(); err != nil {
		log.Printf("[RabbitmqChannelManager] failed to iterate on reconnecting: %v", err)
	}

	return nil
}

func (m *RabbitmqChannelManager) Close() error {
	if err := m.Stop(); err != nil {
		return fmt.Errorf("failed to stop: %w", err)
	}

	if err := m.channel.Close(); err != nil {
		return fmt.Errorf("failed to close channel: %w", err)
	}

	return nil
}

func (m *RabbitmqChannelManager) Send(sendQueue string, payload interface{}, messageID string, correlationID string, replyTo string) error {
	jsonData, err := json.Marshal(payload)
	if err != nil {
		log.Printf("failed to marshal json: %+v", err)
		jsonData = []byte(fmt.Sprintf("%v", payload))
	}

	err = m.channel.Publish(
		"",        // exchange
		sendQueue, // routing key
		false,     // mandatory
		false,     // immediate
		amqp.Publishing{
			ContentType:   "application/json",
			DeliveryMode:  amqp.Persistent,
			MessageId:     messageID,
			CorrelationId: correlationID,
			ReplyTo:       m.queueFrom,
			Body:          []byte(jsonData),
		})
	if err != nil {
		return fmt.Errorf("failed to publish a message: %w", err)
	}

	return nil
}

func (m *RabbitmqChannelManager) QueueFrom() string {
	return m.queueFrom
}

func (m *RabbitmqChannelManager) IsQueueExist(queueName string) bool {
	_, err := m.channel.QueueInspect(queueName)
	return err == nil
}
