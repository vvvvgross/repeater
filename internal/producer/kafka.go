package producer

import (
	"context"
	"errors"
	"strconv"

	"github.com/vvvvgross/repeater/internal/model"
)

var ErrWriterNotConfigured = errors.New("kafka writer is not configured")

type MessageWriter interface {
	Send(ctx context.Context, key, value []byte) error
}

type Sender struct {
	writer MessageWriter
}

func NewSender(writer MessageWriter) *Sender {
	return &Sender{
		writer: writer,
	}
}

func (s *Sender) Send(ctx context.Context, event model.DemoEvent) error {
	if s.writer == nil {
		return ErrWriterNotConfigured
	}

	key := []byte(strconv.FormatInt(event.EntityID, 10))
	value := event.Payload

	return s.writer.Send(ctx, key, value)
}
