package server

import (
	"context"
	"fmt"
	"io/ioutil"
	"os/signal"
	"syscall"

	"github.com/benthosdev/benthos/v4/public/service"

	"github.com/rs/zerolog/log"

	"github.com/turnon/clams/server/api"
	"github.com/turnon/clams/tasklist"
	"github.com/turnon/clams/tasklist/common"

	"gopkg.in/yaml.v3"
)

// config 服务器配置
type config struct {
	Tasklist map[string]any `yaml:"tasklist"`
	Workers  int            `yaml:"workers"`
}

// server 服务器实例
type server struct {
	cfg *config
}

// Run 根据配置启动服务器
func Run(cfgPath string) {
	bytesArr, err := ioutil.ReadFile(cfgPath)
	if err != nil {
		panic(err)
	}

	var cfg config
	err = yaml.Unmarshal(bytesArr, &cfg)
	if err != nil {
		panic(err)
	}

	if cfg.Workers <= 0 {
		cfg.Workers = 1
	}

	srv := server{cfg: &cfg}
	<-srv.run()
}

// log 输出日志
func (srv *server) log(str string, v ...any) {
	log.Debug().Str("mod", "server").Msgf(str, v...)
}

// run 运行服务器
func (srv *server) run() chan struct{} {
	ch := make(chan struct{})
	sigCtx, _ := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	tasks, err := tasklist.NewTaskList(sigCtx, srv.cfg.Tasklist)
	if err != nil {
		fmt.Println(err)
		close(ch)
		return ch
	}

	children := make([]chan struct{}, 0, 1+srv.cfg.Workers)
	children = append(children, api.Interact(sigCtx, tasks))
	for i := 0; i < srv.cfg.Workers; i++ {
		children = append(children, srv.loop(sigCtx, tasks))
	}

	go func() {
		for _, child := range children {
			<-child
		}
		close(ch)
	}()

	return ch
}

// loop 轮询取task执行
func (srv *server) loop(ctx context.Context, taskslist common.Tasklist) chan struct{} {
	ch := make(chan struct{})
	reader := taskslist.NewReader()

	go func() {
		defer close(ch)

		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			task, err := reader.Read(ctx)
			if err != nil {
				srv.log("read task %p %v", task, err)
				continue
			}

			if err := srv.execute(ctx, task); err != nil {
				task.Error(ctx, err)
			} else {
				task.Done(ctx)
			}
		}
	}()

	return ch
}

// execute 执行任务
func (srv *server) execute(ctx context.Context, task common.Task) (err error) {
	srv.log("executeTask start: %v", task.ID())
	defer srv.log("executeTask end: %v, %v", task.ID(), err)

	builder := service.NewStreamBuilder()

	err = builder.SetYAML(task.Description())
	if err != nil {
		return
	}

	stream, err := builder.Build()
	if err != nil {
		return
	}

	err = stream.Run(ctx)
	return
}
