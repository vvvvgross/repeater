package main

import (
	"context"
	"os/signal"
	"syscall"
	"time"

	"github.com/vvvvgross/repeater/internal/app/repeater"
	"github.com/vvvvgross/repeater/internal/consumer"
	"github.com/vvvvgross/repeater/internal/producer"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cfg := repeater.Config{
		ChannelSize:   512,
		ConsumerCount: 2,
		ConsumeSize:   10,
		ProducerCount: 28,
		WorkerCount:   2,
		PollInterval:  200 * time.Millisecond,
		RetryBackoff:  200 * time.Millisecond,
	}

	// Пока реальные PostgreSQL и Kafka не подключены,
	// здесь используются каркасные адаптеры.
	eventRepo := consumer.NewRepo(nil)
	eventSender := producer.NewSender(nil)

	rp := repeater.NewRepeater(cfg, eventRepo, eventSender)
	rp.Start(ctx)

	<-ctx.Done()
	rp.Wait()
}
