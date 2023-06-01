package common

import "context"

type Tasklist interface {
	NewReader() TaskReader
	Write(context.Context, RawTask) error
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
	Done(context.Context) error
	Error(context.Context, error) error
}
