package repeater

import "time"

type Config struct {
	ChannelSize   int
	ConsumerCount int
	ConsumeSize   int
	ProducerCount int
	WorkerCount   int

	PollInterval time.Duration
	RetryBackoff time.Duration
}

func (c Config) normalize() Config {
	if c.ChannelSize <= 0 {
		c.ChannelSize = 128
	}
	if c.ConsumerCount <= 0 {
		c.ConsumerCount = 1
	}
	if c.ConsumeSize <= 0 {
		c.ConsumeSize = 100
	}
	if c.ProducerCount <= 0 {
		c.ProducerCount = 1
	}
	if c.WorkerCount <= 0 {
		c.WorkerCount = 1
	}
	if c.PollInterval <= 0 {
		c.PollInterval = 200 * time.Millisecond
	}
	if c.RetryBackoff <= 0 {
		c.RetryBackoff = 200 * time.Millisecond
	}

	return c
}
