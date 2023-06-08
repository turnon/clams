package local

import (
	"context"
	"os"

	"github.com/benthosdev/benthos/v4/public/service"
	"github.com/rs/zerolog/log"
)

func Run(path string) {
	bytesArr, err := os.ReadFile(path)
	if err != nil {
		logFatal(err)
	}

	builder := service.NewStreamBuilder()

	if err = builder.SetYAML(string(bytesArr)); err != nil {
		logFatal(err)
	}

	stream, err := builder.Build()
	if err != nil {
		logFatal(err)
	}

	if err = stream.Run(context.Background()); err != nil {
		logFatal(err)
	}
}

func logFatal(err error) {
	log.Fatal().Stack().Err(err).Send()
}
