package server

import (
	"context"
	"fmt"
	"io/ioutil"
	"os/signal"
	"syscall"
	"time"

	"github.com/benthosdev/benthos/v4/public/service"

	"github.com/turnon/clams/server/api"
	"github.com/turnon/clams/tasklist"

	"gopkg.in/yaml.v3"
)

type serverConfig struct {
	Tasklist map[string]any `yaml:"tasklist"`
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

	go func() {
		children := []chan struct{}{
			api.Interact(sigCtx, tasks),
			backgroundRun(sigCtx, tasks),
		}
		for _, child := range children {
			<-child
		}
		close(ch)
	}()

	return ch
}

func backgroundRun(ctx context.Context, tasks tasklist.Tasklist) chan struct{} {
	ch := make(chan struct{})

	go func() {
		defer close(ch)

		for {
			select {
			case <-ctx.Done():
				closeCtx, cancelClose := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancelClose()
				err := tasks.Close(closeCtx)
				if err != nil {
					fmt.Println(err)
				}
				fmt.Println("dead")
				return
			case task := <-tasks.Read():
				if err := runTask(ctx, task.Description()); err != nil {
					task.Error(ctx, err)
				} else {
					task.Done(ctx)
				}
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
