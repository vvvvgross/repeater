package model

import "time"

type EventType string

const (
	EventTypeCreated EventType = "created"
)

type EventStatus string

const (
	EventStatusNew        EventStatus = "new"
	EventStatusProcessing EventStatus = "processing"
)

type Demo struct {
	ID   int64
	Name string
}

type DemoEvent struct {
	ID        int64
	EntityID  int64
	Type      EventType
	Payload   []byte
	Status    EventStatus
	CreatedAt time.Time
}
