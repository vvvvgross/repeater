package repo

import (
	"context"

	"github.com/vvvvgross/repeater/internal/model"
)

type EventRepo interface {
	LockBatch(ctx context.Context, limit int) ([]model.DemoEvent, error)
	DeleteBatch(ctx context.Context, ids []int64) error
	MarkForRetryBatch(ctx context.Context, ids []int64) error
}
