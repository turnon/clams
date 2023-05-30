package main

import (
	"flag"
	"fmt"
	"os"

	_ "github.com/benthosdev/benthos/v4/public/components/io"
	_ "github.com/benthosdev/benthos/v4/public/components/pure"
	_ "github.com/benthosdev/benthos/v4/public/service"

	_ "github.com/turnon/clams/input"
	_ "github.com/turnon/clams/output"
	_ "github.com/turnon/clams/processor"
	"github.com/turnon/clams/server"
)

func main() {
	fmt.Printf("pid: %d\n", os.Getpid())

	serverCfgFile := flag.String("s", "server.yml", "server config")
	flag.Parse()

	if *serverCfgFile != "" {
		server.Run(*serverCfgFile)
	}
}
