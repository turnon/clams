package tasklist

import (
	"context"
	"errors"
)

type Tasklist interface {
	Read() chan task
	Write(context.Context, RawTask) error
	Close(context.Context) error
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
