package repeater

import (
	"context"
	"sync"
	"time"

	"github.com/vvvvgross/repeater/internal/model"
	"github.com/vvvvgross/repeater/internal/repo"
	"github.com/vvvvgross/repeater/internal/sender"
)

type Repeater struct {
	cfg    Config
	repo   repo.EventRepo
	sender sender.EventSender

	batchesCh chan []model.DemoEvent
	tasksCh   chan cleanupTask

	startOnce sync.Once
	wg        sync.WaitGroup
}

func NewRepeater(cfg Config, eventRepo repo.EventRepo, eventSender sender.EventSender) *Repeater {
	cfg = cfg.normalize()

	return &Repeater{
		cfg:    cfg,
		repo:   eventRepo,
		sender: eventSender,

		batchesCh: make(chan []model.DemoEvent, cfg.ChannelSize),
		tasksCh:   make(chan cleanupTask, cfg.ChannelSize),
	}
}

func (r *Repeater) Start(ctx context.Context) {
	r.startOnce.Do(func() {
		var consumerWG sync.WaitGroup
		var producerWG sync.WaitGroup

		for i := 0; i < r.cfg.WorkerCount; i++ {
			r.wg.Add(1)
			go func() {
				defer r.wg.Done()
				r.worker(ctx)
			}()
		}

		for i := 0; i < r.cfg.ProducerCount; i++ {
			r.wg.Add(1)
			producerWG.Add(1)
			go func() {
				defer r.wg.Done()
				defer producerWG.Done()
				r.producer(ctx)
			}()
		}

		r.wg.Add(1)
		go func() {
			defer r.wg.Done()
			producerWG.Wait()
			close(r.tasksCh)
		}()

		for i := 0; i < r.cfg.ConsumerCount; i++ {
			r.wg.Add(1)
			consumerWG.Add(1)
			go func() {
				defer r.wg.Done()
				defer consumerWG.Done()
				r.consumer(ctx)
			}()
		}

		r.wg.Add(1)
		go func() {
			defer r.wg.Done()
			consumerWG.Wait()
			close(r.batchesCh)
		}()
	})
}

func (r *Repeater) Wait() {
	r.wg.Wait()
}

func (r *Repeater) consumer(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}

		batch, err := r.repo.LockBatch(ctx, r.cfg.ConsumeSize)
		if err != nil {
			if !sleepWithContext(ctx, r.cfg.RetryBackoff) {
				return
			}
			continue
		}

		if len(batch) == 0 {
			if !sleepWithContext(ctx, r.cfg.PollInterval) {
				return
			}
			continue
		}

		select {
		case <-ctx.Done():
			return
		case r.batchesCh <- batch:
		}
	}
}

func (r *Repeater) producer(ctx context.Context) {
	select {
	case <-ctx.Done():
		return
	case batch, ok := <-r.batchesCh:
		if !ok {
			return
		}

		task := cleanupTask{
			SuccessIDs: make([]int64, len(batch)),
			RetryIDs:   make([]int64, len(batch)),
		}

		for i, event := range batch {
			err := r.sender.Send(ctx, event)

			if err != nil {
				task.RetryIDs = appendEventIDs(task.RetryIDs, batch[i:]...)
				break
			}

			task.SuccessIDs = append(task.SuccessIDs, event.ID)
		}

		// if len(task.SuccessIDs) == 0 && len(task.RetryIDs) == 0 {
		// }

		select {
		case <-ctx.Done():
			return
		case r.tasksCh <- task:
		}

	}
}

func (r *Repeater) worker(ctx context.Context) {
	select {
	case <-ctx.Done():
		return
	case task, ok := <-r.tasksCh:
		if !ok {
			return
		}

		for {
			err := r.handleCleanupTask(ctx, task)
			if err == nil {
				break
			}

			if !sleepWithContext(ctx, r.cfg.RetryBackoff) {
				return
			}
		}
	}
}

func sleepWithContext(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func appendEventIDs(dst []int64, events ...model.DemoEvent) []int64 {
	ids := make([]int64, len(events))

	for _, val := range events {
		ids = append(ids, val.ID)
	}

	return ids
}

func (r *Repeater) handleCleanupTask(ctx context.Context, task cleanupTask) error {
	retryIDs := make([]int64, len(task.RetryIDs))
	copy(retryIDs, task.RetryIDs)

	if len(task.SuccessIDs) > 0 {
		err := r.repo.DeleteBatch(ctx, task.SuccessIDs)

		if err != nil {
			retryIDs = append(retryIDs, task.SuccessIDs...)
		}
	}

	if len(retryIDs) > 0 {
		err := r.repo.MarkForRetryBatch(ctx, retryIDs)

		if err != nil {
			return err
		}
	}

	return nil
}
