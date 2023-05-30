package tasklist

import "context"

type Tasklist interface {
	Read() chan task
	Write(context.Context, RawTask) error
	Close(context.Context) error
}

type RawTask struct {
}

type task interface {
	Description() string
	Done(context.Context) error
}

func NewTaskList(ctx context.Context, cfg map[string]any) (Tasklist, error) {
	if cfg["type"] == "pg" {
		return initPgTasklist(ctx, cfg)
	}
	return nil, nil
}
