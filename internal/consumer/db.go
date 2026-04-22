package consumer

import (
	"context"
	"database/sql"
	"errors"

	"github.com/vvvvgross/repeater/internal/model"
)

var ErrNotImplemented = errors.New("postgres repo is not implemented yet")

type Repo struct {
	db *sql.DB
}

func NewRepo(db *sql.DB) *Repo {
	return &Repo{
		db: db,
	}
}

func (r *Repo) LockBatch(ctx context.Context, limit int) ([]model.DemoEvent, error) {
	// (что надо сделать в будущем при подключении БД)
	// Основная логика:

	// 1. Начать транзакцию
	// 2. Выбрать limit событий в статусе "new"
	// 3. Использовать FOR UPDATE SKIP LOCKED
	// 4. Перевести выбранные записи в статус "processing"
	// 5. Закоммитить транзакцию
	// 6. Вернуть батч событий

	_ = ctx
	_ = limit

	return nil, ErrNotImplemented
}

func (r *Repo) DeleteBatch(ctx context.Context, ids []int64) error {
	// (что надо сделать в будущем при подключении БД)
	// Основная логика:

	// DELETE FROM events WHERE id IN (...)

	_ = ctx
	_ = ids

	return ErrNotImplemented
}

func (r *Repo) MarkForRetryBatch(ctx context.Context, ids []int64) error {
	// (что надо сделать в будущем при подключении БД)
	// Основная логика:

	// UPDATE events
	// SET status = 'new'
	// WHERE id IN (...)

	_ = ctx
	_ = ids

	return ErrNotImplemented
}
