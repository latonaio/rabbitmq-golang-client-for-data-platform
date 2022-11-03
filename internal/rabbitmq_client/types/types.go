package types

// RabbitmqConfig は、RabbitMQ への接続情報を保持するインターフェースです。
type RabbitmqConfig interface {
	// URL は、接続先 RabbitMQ の情報を URL 形式で返します。
	URL() string
	// QueueFrom は、通常のメッセージを受信するキューの名前を返します。
	QueueFrom() string
	// SessionKeepResponseQueue は、レスポンスメッセージを受信するキューの名前を返します。
	SessionResponseQueue() string
	// QueueTo は、送信するキューの名前の配列を返します。
	QueueTo() []string
	// PrefetchCount は、通常のメッセージを受信する際のプリフェッチ数を返します。
	PrefetchCount() int
}

// // RabbitmqClient は、RabbitMQ への接続を管理するインターフェースです。
// type RabbitmqClient interface {
// 	// Close は、RabbitMQ への接続を終了します。
// 	Close() error
// 	// Stop は、Iterator での受信を終了します。
// 	Stop() error
// 	// Iterator は、通常のメッセージの受信を開始します。
// 	Iterator() (<-chan RabbitmqMessage, error)
// 	// Send は、通常のメッセージを送信します。
// 	Send(sendQueue string, payload map[string]interface{}) error
// 	// SessionKeepRequest は、リクエストメッセージを送信し、レスポンスメッセージを受信するまで待機します。
// 	SessionKeepRequest(ctx context.Context, sendQueue string, payload map[string]interface{}) (RabbitmqMessage, error)
// }

// RabbitmqMessage は、RabbitMQ から受信したメッセージを保持するインターフェースです。
type RabbitmqMessage interface {
	// QueueName は、受信したキューの名前を取得します。
	QueueName() string
	// Data は、メッセージのペイロードを取得します。
	Data() map[string]interface{}
	// Raw は、メッセージのペイロードを取得します。
	Raw() []byte
	// Respond は、レスポンスメッセージを送信します。
	Respond(payload interface{}) error
	// Success は、メッセージ処理の成功応答を返します。
	Success() error
	// Fail は、メッセージ処理の失敗応答を返します。
	Fail() error
	// Requeue は、メッセージ処理の失敗応答を返し、キューにメッセージを戻します。
	Requeue() error
	// MessageID は、メッセージの ID を取得します。
	MessageID() string
	// CorrelationID は、返信元のリクエストメッセージの ID を取得します。
	CorrelationID() string
	// IsResponded は、レスポンスメッセージを送信し終えたかどうかを返します。
	IsResponded() bool
	// IsAcked は、メッセージ処理結果の応答の送信が済んでいるかを返します。
	IsAcked() bool
	// IsRequest は、メッセージがリクエストかどうかを返します。
	IsRequest() bool
}
