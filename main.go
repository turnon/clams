package main

import (
	"context"
	"fmt"
	"os"

	"github.com/benthosdev/benthos/v4/public/service"

	_ "github.com/benthosdev/benthos/v4/public/components/io"
	_ "github.com/benthosdev/benthos/v4/public/components/pure"

	_ "github.com/turnon/clams/input"
	_ "github.com/turnon/clams/output"
	_ "github.com/turnon/clams/processor"
)

func main() {
	fmt.Printf("pid: %d\n", os.Getpid())
	service.RunCLI(context.Background())
}
