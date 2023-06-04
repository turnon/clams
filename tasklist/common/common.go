package common

import "context"

type Tasklist interface {
	NewReader() TaskReader
	Write(context.Context, RawTask) error
	Delete(context.Context, string) error
	Close(context.Context) error
}

type TaskReader interface {
	Read(context.Context) (Task, error)
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
