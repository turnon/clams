package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/benthosdev/benthos/v4/public/service"

	_ "github.com/benthosdev/benthos/v4/public/components/io"
	_ "github.com/benthosdev/benthos/v4/public/components/pure"

	_ "github.com/turnon/clams/input"
	_ "github.com/turnon/clams/output"
	_ "github.com/turnon/clams/processor"
	"github.com/turnon/clams/task"
)

func main() {
	fmt.Printf("pid: %d\n", os.Getpid())
	// service.RunCLI(context.Background())

	<-runUntilKilled()
}

func runUntilKilled() chan struct{} {
	ch := make(chan struct{})
	sigCtx, _ := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)

	go func() {
		defer close(ch)

		for {
			select {
			case <-sigCtx.Done():
				<-time.After(5 * time.Second)
				fmt.Println("dead")
				return
			default:
				fetchJob(sigCtx)
			}
		}
	}()

	return ch
}

func fetchJob(ctx context.Context) {
	t := task.New()
	taskDescription, err := t.Load(ctx)
	if err != nil {
		fmt.Println(err)
		return
	}

	if taskDescription == "" {
		<-time.After(5 * time.Second)
		return
	}

	fmt.Println(taskDescription)

	err = t.Done(ctx)
	if err != nil {
		fmt.Println(err)
	}

	fmt.Println(time.Now())
}
