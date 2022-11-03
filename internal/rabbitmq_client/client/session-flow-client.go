package client

import "encoding/json"

type SessionFlowClient struct {
	rmq       *RabbitmqClient
	sessionID string
}

func (c *RabbitmqClient) NewSessionFlowClient(sessionID string) *SessionFlowClient {
	return &SessionFlowClient{
		rmq:       c,
		sessionID: sessionID,
	}
}

func (c *SessionFlowClient) Send(sendQueue string, payload interface{}) error {
	b, err := json.Marshal(payload)
	if err != nil {
		return c.rmq.Send(sendQueue, map[string]interface{}{
			"runtime_session_id": c.sessionID,
			"message":            payload,
		})
	}
	m := map[string]interface{}{}
	json.Unmarshal(b, &m)
	m["runtime_session_id"] = c.sessionID
	return c.rmq.Send(sendQueue, m)
}
