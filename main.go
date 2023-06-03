package main

import (
	"flag"
	"os"

	_ "github.com/benthosdev/benthos/v4/public/components/io"
	_ "github.com/benthosdev/benthos/v4/public/components/pure"
	_ "github.com/benthosdev/benthos/v4/public/service"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	_ "github.com/turnon/clams/input"
	"github.com/turnon/clams/local"
	_ "github.com/turnon/clams/output"
	_ "github.com/turnon/clams/processor"
	"github.com/turnon/clams/server"
)

func main() {

	serverCfgFile := flag.String("server", "", "server config")
	localCfgFile := flag.String("local", "", "run locally")
	debug := flag.Bool("debug", false, "sets log level to debug")
	flag.Parse()

	zerolog.SetGlobalLevel(zerolog.InfoLevel)
	if *debug {
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	}
	log.Debug().Int("pid", os.Getpid()).Send()

	if *serverCfgFile != "" {
		server.Run(*serverCfgFile)
		return
	}

	if *localCfgFile != "" {
		local.Run(*localCfgFile)
		return
	}
}
