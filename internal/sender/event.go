package sender

import (
	"context"

	"github.com/vvvvgross/repeater/internal/model"
)

type EventSender interface {
	Send(ctx context.Context, event model.DemoEvent) error
}
