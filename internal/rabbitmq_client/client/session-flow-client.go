package client

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

func (c *SessionFlowClient) Send(sendQueue string, payload map[string]interface{}) error {
	payload["runtime_session_id"] = c.sessionID
	return c.rmq.Send(sendQueue, payload)
}
