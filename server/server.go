package server

import (
	"context"
	"fmt"
	"io/ioutil"
	"os/signal"
	"syscall"

	"github.com/benthosdev/benthos/v4/public/service"

	"github.com/turnon/clams/server/api"
	"github.com/turnon/clams/tasklist"
	"github.com/turnon/clams/tasklist/common"

	"gopkg.in/yaml.v3"
)

type serverConfig struct {
	Tasklist map[string]any `yaml:"tasklist"`
	Workers  int            `yaml:"workers"`
}

func Run(cfgPath string) {
	bytesArr, err := ioutil.ReadFile(cfgPath)
	if err != nil {
		panic(err)
	}

	var serverCfg serverConfig
	err = yaml.Unmarshal(bytesArr, &serverCfg)
	if err != nil {
		panic(err)
	}

	if serverCfg.Workers <= 0 {
		serverCfg.Workers = 1
	}

	<-runServers(&serverCfg)
}

func runServers(serverCfg *serverConfig) chan struct{} {
	ch := make(chan struct{})
	sigCtx, _ := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	tasks, err := tasklist.NewTaskList(sigCtx, serverCfg.Tasklist)
	if err != nil {
		fmt.Println(err)
		close(ch)
		return ch
	}

	children := make([]chan struct{}, 0, 1+serverCfg.Workers)
	children = append(children, api.Interact(sigCtx, tasks))
	for i := 0; i < serverCfg.Workers; i++ {
		children = append(children, backgroundRun(sigCtx, tasks))
	}

	go func() {
		for _, child := range children {
			<-child
		}
		close(ch)
	}()

	return ch
}

// 轮询取task执行
func backgroundRun(ctx context.Context, taskslist common.Tasklist) chan struct{} {
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
				fmt.Println(err)
				continue
			}

			if err := runTask(ctx, task.Description()); err != nil {
				task.Error(ctx, err)
			} else {
				task.Done(ctx)
			}
		}
	}()

	return ch
}

func runTask(ctx context.Context, str string) error {
	builder := service.NewStreamBuilder()

	err := builder.SetYAML(str)
	if err != nil {
		return err
	}

	stream, err := builder.Build()
	if err != nil {
		return err
	}

	return stream.Run(ctx)
}
