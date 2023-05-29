package task

import "context"

type Task interface {
	Connect(context.Context)
	Load(context.Context) (string, error)
	Done(context.Context) error
}

func New() Task {
	return &sqliteTask{}
}
