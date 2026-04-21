package repeater

import "context"

type Repeater struct{}

func NewRepaeter() {}

func (r *Repeater) Start(ctx context.Context) {}

func (r *Repeater) Wait() {}

func (r *Repeater) consumer() {}

func (r *Repeater) producer() {}

func (r *Repeater) worker() {}
