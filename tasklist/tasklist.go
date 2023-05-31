package tasklist

import (
	"context"
	"errors"
)

type Tasklist interface {
	NewReader() taskReader
	// Read() chan task
	Write(context.Context, RawTask) error
	Close(context.Context) error
}

type taskReader interface {
	Read(context.Context) (task, error)
}

type RawTask struct {
	Description string
	ScheduledAt string
}

type task interface {
	Description() string
	Done(context.Context) error
	Error(context.Context, error) error
}

func NewTaskList(ctx context.Context, cfg map[string]any) (Tasklist, error) {
	if cfg["type"] == "pg" {
		return initPgTasklist(ctx, cfg)
	}
	return nil, errors.New("no tasklist config")
}
