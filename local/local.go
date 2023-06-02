package local

import (
	"context"
	"io/ioutil"

	"github.com/benthosdev/benthos/v4/public/service"
)

func Run(path string) {
	bytesArr, err := ioutil.ReadFile(path)
	if err != nil {
		panic(err)
	}

	builder := service.NewStreamBuilder()

	if err = builder.SetYAML(string(bytesArr)); err != nil {
		panic(err)
	}

	stream, err := builder.Build()
	if err != nil {
		panic(err)
	}

	if err = stream.Run(context.Background()); err != nil {
		panic(err)
	}
}
