package server

import (
	"context"
	"errors"
	"os"
	"strconv"

	"github.com/benthosdev/benthos/v4/public/service"
	"github.com/rs/zerolog/log"
	"github.com/turnon/clams/tasklist/common"
)

// workteam 工作组
type workteam struct {
	workers []*taskWorker
	running chan struct{}
}

// newWorkteam 创建工作组
func newWorkteam(ctx context.Context, taskslist common.Tasklist, workerCount int) *workteam {
	team := &workteam{
		workers: make([]*taskWorker, 0, workerCount),
		running: make(chan struct{}),
	}

	for i := 0; i < workerCount; i++ {
		team.workers = append(team.workers, newTaskWorker(ctx, i, taskslist))
	}

	go func() {
		for _, w := range team.workers {
			<-w.running
		}
		close(team.running)
	}()

	return team
}

// wait 等待worker退出
func (team *workteam) wait() chan struct{} {
	return team.running
}

// taskWorker worker
type taskWorker struct {
	ctx       context.Context
	taskslist common.Tasklist
	id        string
	running   chan struct{}
}

// newTaskWorker 创建worker
func newTaskWorker(ctx context.Context, idx int, taskslist common.Tasklist) *taskWorker {
	hostname, _ := os.Hostname()
	id := hostname + ":" + strconv.Itoa(os.Getpid()) + ":" + strconv.Itoa(idx)
	worker := &taskWorker{taskslist: taskslist, ctx: ctx, id: id}
	worker.loop()
	return worker
}

// logDebug 输出日志
func (worker *taskWorker) logDebug(str string, v ...any) {
	log.Debug().Str("mod", "taskWorker").Str("id", worker.id).Msgf(str, v...)
}

// logInfo 输出日志
func (worker *taskWorker) logInfo(str string, v ...any) {
	log.Info().Str("mod", "taskWorker").Str("id", worker.id).Msgf(str, v...)
}

// loop 轮询取task执行
func (worker *taskWorker) loop() {
	worker.running = make(chan struct{})

	go func() {
		defer close(worker.running)

		for {
			task, err := worker.taskslist.Read(worker.ctx)
			if errors.Is(err, context.Canceled) {
				return
			}
			if err != nil {
				worker.logDebug("read task %p %v", task, err)
				continue
			}

			worker.execute(task)
		}
	}()
}

// execute 执行任务
func (worker *taskWorker) execute(task common.Task) {
	var err error

	worker.logInfo("executeTask start: %v", task.ID())
	defer worker.logInfo("executeTask end: %v, %v", task.ID(), err)

	builder := service.NewStreamBuilder()

	err = builder.SetYAML(task.Description())
	if err != nil {
		task.Error(worker.ctx, err)
		return
	}

	stream, err := builder.Build()
	if err != nil {
		task.Error(worker.ctx, err)
		return
	}

	// listen to abort
	ctx, cancel := context.WithCancel(worker.ctx)
	defer cancel()

	go func() {
		select {
		case <-ctx.Done():
		case <-task.Aborted():
			stream.Stop(context.Background())
		}
		cancel()
	}()

	err = stream.Run(ctx)
	if err != nil {
		task.Error(worker.ctx, err)
		return
	}

	task.Done(ctx)
}
