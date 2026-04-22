package repeater

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vvvvgross/repeater/internal/model"
)

type repoMock struct {
	lockBatchFn         func(ctx context.Context, limit int) ([]model.DemoEvent, error)
	deleteBatchFn       func(ctx context.Context, ids []int64) error
	markForRetryBatchFn func(ctx context.Context, ids []int64) error
}

func (m *repoMock) LockBatch(ctx context.Context, limit int) ([]model.DemoEvent, error) {
	return m.lockBatchFn(ctx, limit)
}

func (m *repoMock) DeleteBatch(ctx context.Context, ids []int64) error {
	return m.deleteBatchFn(ctx, ids)
}

func (m *repoMock) MarkForRetryBatch(ctx context.Context, ids []int64) error {
	return m.markForRetryBatchFn(ctx, ids)
}

type senderMock struct {
	sendFn func(ctx context.Context, event model.DemoEvent) error
}

func (m *senderMock) Send(ctx context.Context, event model.DemoEvent) error {
	return m.sendFn(ctx, event)
}

func testConfig() Config {
	return Config{
		ChannelSize:   16,
		ConsumerCount: 1,
		ConsumeSize:   10,
		ProducerCount: 1,
		WorkerCount:   1,
		PollInterval:  10 * time.Millisecond,
		RetryBackoff:  10 * time.Millisecond,
	}
}

func TestRepeater_SuccessFlow(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var lockCalls int32
	deleteCalled := make(chan []int64, 1)

	repo := &repoMock{
		lockBatchFn: func(ctx context.Context, limit int) ([]model.DemoEvent, error) {
			call := atomic.AddInt32(&lockCalls, 1)

			if call == 1 {
				return []model.DemoEvent{
					{ID: 1},
					{ID: 2},
				}, nil
			}

			<-ctx.Done()
			return nil, ctx.Err()
		},
		deleteBatchFn: func(ctx context.Context, ids []int64) error {
			deleteCalled <- append([]int64(nil), ids...)
			cancel()
			return nil
		},
		markForRetryBatchFn: func(ctx context.Context, ids []int64) error {
			t.Fatalf("unexpected retry call: %v", ids)
			return nil
		},
	}

	sender := &senderMock{
		sendFn: func(ctx context.Context, event model.DemoEvent) error {
			return nil
		},
	}

	rp := NewRepeater(testConfig(), repo, sender)
	rp.Start(ctx)

	select {
	case ids := <-deleteCalled:
		if !reflect.DeepEqual(ids, []int64{1, 2}) {
			t.Fatalf("unexpected delete ids: %v", ids)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting delete call")
	}

	rp.Wait()
}

func TestRepeater_SendError_RetryTail(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var lockCalls int32
	done := make(chan struct{}, 1)

	var deleted []int64
	var retried []int64
	var mu sync.Mutex

	repo := &repoMock{
		lockBatchFn: func(ctx context.Context, limit int) ([]model.DemoEvent, error) {
			call := atomic.AddInt32(&lockCalls, 1)

			if call == 1 {
				return []model.DemoEvent{
					{ID: 1},
					{ID: 2},
					{ID: 3},
				}, nil
			}

			<-ctx.Done()
			return nil, ctx.Err()
		},
		deleteBatchFn: func(ctx context.Context, ids []int64) error {
			mu.Lock()
			deleted = append([]int64(nil), ids...)
			mu.Unlock()
			return nil
		},
		markForRetryBatchFn: func(ctx context.Context, ids []int64) error {
			mu.Lock()
			retried = append([]int64(nil), ids...)
			mu.Unlock()

			done <- struct{}{}
			cancel()
			return nil
		},
	}

	sender := &senderMock{
		sendFn: func(ctx context.Context, event model.DemoEvent) error {
			if event.ID == 2 {
				return errors.New("send failed")
			}
			return nil
		},
	}

	rp := NewRepeater(testConfig(), repo, sender)
	rp.Start(ctx)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("timeout waiting retry call")
	}

	rp.Wait()

	mu.Lock()
	defer mu.Unlock()

	if !reflect.DeepEqual(deleted, []int64{1}) {
		t.Fatalf("unexpected deleted ids: %v", deleted)
	}

	if !reflect.DeepEqual(retried, []int64{2, 3}) {
		t.Fatalf("unexpected retried ids: %v", retried)
	}
}

func TestRepeater_DeleteError_FallbackToRetry(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var lockCalls int32
	retriedCh := make(chan []int64, 1)

	repo := &repoMock{
		lockBatchFn: func(ctx context.Context, limit int) ([]model.DemoEvent, error) {
			call := atomic.AddInt32(&lockCalls, 1)

			if call == 1 {
				return []model.DemoEvent{
					{ID: 10},
				}, nil
			}

			<-ctx.Done()
			return nil, ctx.Err()
		},
		deleteBatchFn: func(ctx context.Context, ids []int64) error {
			return errors.New("delete failed")
		},
		markForRetryBatchFn: func(ctx context.Context, ids []int64) error {
			retriedCh <- append([]int64(nil), ids...)
			cancel()
			return nil
		},
	}

	sender := &senderMock{
		sendFn: func(ctx context.Context, event model.DemoEvent) error {
			return nil
		},
	}

	rp := NewRepeater(testConfig(), repo, sender)
	rp.Start(ctx)

	select {
	case ids := <-retriedCh:
		if !reflect.DeepEqual(ids, []int64{10}) {
			t.Fatalf("unexpected retry ids: %v", ids)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting retry call")
	}

	rp.Wait()
}
