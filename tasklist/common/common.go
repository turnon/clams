package common

import "context"

type Tasklist interface {
	Read(context.Context) (Task, error)
	Write(context.Context, RawTask) error
	Delete(context.Context, string) error
	Close(context.Context) error
}

type RawTask struct {
	Description string
	ScheduledAt string
}

type Task interface {
	ID() string
	Description() string
	Aborted() chan struct{}
	Done(context.Context) error
	Error(context.Context, error) error
}
