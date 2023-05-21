package stdoutvertical

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/benthosdev/benthos/v4/public/service"
)

func init() {
	err := service.RegisterOutput(
		"stdoutvertical",
		service.NewConfigSpec().Field(service.NewBoolField("meta").Default(false)),
		func(conf *service.ParsedConfig, mgr *service.Resources) (out service.Output, maxInFlight int, err error) {
			meta, err := conf.FieldBool("meta")
			if err != nil {
				return nil, 1, err
			}
			return &stdoutvertical{meta: meta}, 1, nil
		},
	)
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

type stdoutvertical struct {
	count uint64
	meta  bool
}

func (stdver *stdoutvertical) Connect(ctx context.Context) error {
	return nil
}

func (stdver *stdoutvertical) Write(ctx context.Context, msg *service.Message) error {
	structed, err := msg.AsStructured()
	if err != nil {
		return err
	}

	asMap := structed.(map[string]any)

	stdver.count += 1

	sb := &strings.Builder{}
	sb.WriteString("Row ")
	sb.WriteString(strconv.Itoa(int(stdver.count)))
	sb.WriteString(":")
	titleLen := sb.Len()
	sb.WriteString("\n")
	for i := titleLen; i > 0; i-- {
		sb.WriteString("-")
	}
	sb.WriteString("\n")
	for k, v := range asMap {
		sb.WriteString(k)
		sb.WriteString(": ")
		vStr := fmt.Sprintf("%v", v)
		sb.WriteString(vStr)
		sb.WriteString("\n")
	}

	if stdver.meta {
		sb.WriteString("---\n")
		msg.MetaWalk(func(k string, v string) error {
			sb.WriteString(k)
			sb.WriteString(": ")
			sb.WriteString(v)
			sb.WriteString("\n")
			return nil
		})
	}

	sb.WriteString("\n")

	fmt.Println(sb)
	return nil
}

func (stdver *stdoutvertical) Close(ctx context.Context) error {
	return nil
}
