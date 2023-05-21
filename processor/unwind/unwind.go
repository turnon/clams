package unwind

import (
	"context"
	"encoding/json"

	"github.com/benthosdev/benthos/v4/public/service"
)

func init() {
	configSpec := service.NewConfigSpec().
		Field(service.NewStringField("field"))

	constructor := func(conf *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
		field, err := conf.FieldString("field")
		if err != nil {
			return nil, err
		}
		return &unwindProcessor{field: field}, nil
	}

	err := service.RegisterProcessor("unwind", configSpec, constructor)
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

type unwindProcessor struct {
	field string
}

func (un *unwindProcessor) Process(ctx context.Context, msg *service.Message) (service.MessageBatch, error) {
	structed, err := msg.AsStructured()
	if err != nil {
		return nil, err
	}
	msgAsMap := structed.(map[string]any)
	valueMaybeArray := msgAsMap[un.field]
	if valueMaybeArray == nil {
		return []*service.Message{msg}, nil
	}

	valueAsArray := valueMaybeArray.([]any)
	msgs := make([]*service.Message, 0, len(valueAsArray))
	for _, element := range valueAsArray {
		msgAsMap[un.field] = element
		bytes, err := json.Marshal(msgAsMap)
		if err != nil {
			return []*service.Message{}, err
		}
		newMsg := service.NewMessage(bytes)
		msgs = append(msgs, newMsg)
	}

	return msgs, nil
}

func (r *unwindProcessor) Close(ctx context.Context) error {
	return nil
}
