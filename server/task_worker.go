package server

import (
	"context"
	"errors"

	"github.com/benthosdev/benthos/v4/public/service"
	"github.com/rs/zerolog/log"
	"github.com/turnon/clams/tasklist/common"
)

type taskWorker struct {
	ctx       context.Context
	taskslist common.Tasklist
	idx       int
	ch        chan struct{}
}

func newTaskWorker(ctx context.Context, idx int, taskslist common.Tasklist) *taskWorker {
	worker := &taskWorker{taskslist: taskslist, ctx: ctx, idx: idx}
	worker.loop()
	return worker
}

// wait 等待worker退出
func (worker *taskWorker) wait() chan struct{} {
	return worker.ch
}

// logDebug 输出日志
func (worker *taskWorker) logDebug(str string, v ...any) {
	log.Debug().Str("mod", "taskWorker").Int("idx", worker.idx).Msgf(str, v...)
}

// logInfo 输出日志
func (worker *taskWorker) logInfo(str string, v ...any) {
	log.Info().Str("mod", "taskWorker").Int("idx", worker.idx).Msgf(str, v...)
}

// loop 轮询取task执行
func (worker *taskWorker) loop() {
	worker.ch = make(chan struct{})
	reader := worker.taskslist.NewReader()

	go func() {
		defer close(worker.ch)

		for {
			task, err := reader.Read(worker.ctx)
			if errors.Is(err, context.Canceled) {
				return
			}
			if err != nil {
				worker.logDebug("read task %p %v", task, err)
				continue
			}

			worker.execute(worker.ctx, task)
		}
	}()
}

// execute 执行任务
func (worker *taskWorker) execute(ctx context.Context, task common.Task) {
	var err error

	worker.logInfo("executeTask start: %v", task.ID())
	defer worker.logInfo("executeTask end: %v, %v", task.ID(), err)

	builder := service.NewStreamBuilder()

	err = builder.SetYAML(task.Description())
	if err != nil {
		task.Error(ctx, err)
		return
	}

	stream, err := builder.Build()
	if err != nil {
		task.Error(ctx, err)
		return
	}

	err = stream.Run(ctx)
	if err != nil {
		task.Error(ctx, err)
		return
	}

	task.Done(ctx)
}
