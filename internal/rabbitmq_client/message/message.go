package message

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/latonaio/rabbitmq-golang-client-for-data-platform/internal/rabbitmq_client/types"
	"github.com/streadway/amqp"
)

type rabbitmqChannelManager interface {
	Send(sendQueue string, payload map[string]interface{}, messageID string, correlationID string, replyTo string) error
}

type rabbitmqMessage struct {
	data        map[string]interface{}
	message     amqp.Delivery
	queueName   string
	chManager   rabbitmqChannelManager
	isAcked     bool
	isResponded bool
}

func decodeJSON(body []byte) (map[string]interface{}, error) {
	jsonData := map[string]interface{}{}
	if err := json.Unmarshal(body, &jsonData); err != nil {
		return nil, fmt.Errorf("failed to decode as JSON: %w", err)
	}

	return jsonData, nil
}

func NewRabbitmqMessage(d amqp.Delivery, queueName string, chManager rabbitmqChannelManager) (types.RabbitmqMessage, error) {
	jsonData, err := decodeJSON(d.Body)
	if err != nil {
		return nil, err
	}

	return &rabbitmqMessage{data: jsonData, message: d, chManager: chManager}, nil
}

func (m *rabbitmqMessage) QueueName() string {
	return m.queueName
}

func (m *rabbitmqMessage) Data() map[string]interface{} {
	return m.data
}

func (m *rabbitmqMessage) Respond(payload map[string]interface{}) error {
	if m.isResponded {
		log.Println("[WARNING] already responded")
		return nil
	}
	if err := m.chManager.Send(m.message.ReplyTo, payload, "", m.message.MessageId, ""); err != nil {
		return fmt.Errorf("failed to respond: %w", err)
	}
	m.isResponded = true
	return nil
}

func (m *rabbitmqMessage) Success() error {
	if err := m.message.Ack(false); err != nil {
		return fmt.Errorf("failed to respond with success: %w", err)
	}
	m.isAcked = true
	return nil
}

func (m *rabbitmqMessage) Fail() error {
	if err := m.message.Nack(false, false); err != nil {
		return fmt.Errorf("failed to respond with fail: %w", err)
	}
	m.isAcked = true
	return nil
}

func (m *rabbitmqMessage) Requeue() error {
	if err := m.message.Nack(false, true); err != nil {
		return fmt.Errorf("failed to respond with requeue: %w", err)
	}
	m.isAcked = true
	return nil
}

func (m *rabbitmqMessage) IsResponded() bool {
	return m.isResponded
}

func (m *rabbitmqMessage) IsAcked() bool {
	return m.isAcked
}

func (m *rabbitmqMessage) IsRequest() bool {
	return m.message.MessageId != "" && m.message.ReplyTo != ""
}

func (m *rabbitmqMessage) MessageID() string {
	return m.message.MessageId
}

func (m *rabbitmqMessage) CorrelationID() string {
	return m.message.CorrelationId
}
