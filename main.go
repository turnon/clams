package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/benthosdev/benthos/v4/public/components/io"
	_ "github.com/benthosdev/benthos/v4/public/components/pure"
	_ "github.com/benthosdev/benthos/v4/public/service"

	"github.com/turnon/clams/api"
	_ "github.com/turnon/clams/input"
	_ "github.com/turnon/clams/output"
	_ "github.com/turnon/clams/processor"
	"github.com/turnon/clams/tasklist"
)

func main() {
	fmt.Printf("pid: %d\n", os.Getpid())
	// service.RunCLI(context.Background())

	<-runServers()
}

func runServers() chan struct{} {
	ch := make(chan struct{})
	sigCtx, _ := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	tasks, err := tasklist.NewTaskList(sigCtx, map[string]any{"type": "pg"})
	if err != nil {
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
				fmt.Println(task.Description())
				task.Done(ctx)
			}
		}
	}()

	return ch
}
