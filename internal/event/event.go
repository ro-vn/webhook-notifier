package event

import (
	"encoding/json"
	"time"
)

const TopicName = "webhook-events"

type Event struct {
	EventID   string          `json:"event_id"`
	AccountID string          `json:"account_id"`
	EventType string          `json:"event_type"`
	Payload   json.RawMessage `json:"payload"`
	CreatedAt time.Time       `json:"created_at"`
}

type SubscriberPayload struct {
	Email     string `json:"email"`
	FirstName string `json:"first_name"`
	LastName  string `json:"last_name"`
}
