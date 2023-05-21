package json2cols

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"

	"github.com/benthosdev/benthos/v4/public/service"
)

func init() {
	configSpec := service.NewConfigSpec().
		Field(service.NewStringField("field")).
		Field(service.NewBoolField("keep").Default(false))

	constructor := func(conf *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
		field, err := conf.FieldString("field")
		if err != nil {
			return nil, err
		}
		keep, err := conf.FieldBool("keep")
		if err != nil {
			return nil, err
		}
		return &json2cols{field: field, keep: keep}, nil
	}

	err := service.RegisterProcessor("json2cols", configSpec, constructor)
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

type json2cols struct {
	field string
	keep  bool
}

func (js2cols *json2cols) Process(ctx context.Context, msg *service.Message) (service.MessageBatch, error) {
	structed, err := msg.AsStructured()
	if err != nil {
		return nil, err
	}

	// try to destruct that value to map or slice
	msgAsMap := structed.(map[string]any)
	value := msgAsMap[js2cols.field]
	var destructedValue any
	if reflect.TypeOf(value).Name() == "string" {
		valueAsStr := value.(string)
		if strings.HasPrefix(valueAsStr, "{") && strings.HasSuffix(valueAsStr, "}") {
			aMap := make(map[string]any)
			json.Unmarshal([]byte(valueAsStr), &aMap)
			destructedValue = aMap
		} else if strings.HasPrefix(valueAsStr, "[") && strings.HasSuffix(valueAsStr, "]") {
			aArr := []any{}
			json.Unmarshal([]byte(valueAsStr), &aArr)
			destructedValue = aArr
		} else {
			destructedValue = valueAsStr
		}
	} else {
		destructedValue = value
	}

	// flatten and get types
	msgAsMap[js2cols.field] = destructedValue
	attrs, types := flattenMap(msgAsMap)

	// if js2cols.keep == true {
	// 	msgAsMap[js2cols.field] = value
	// }

	// make a new message
	attrsBytes, err := json.Marshal(attrs)
	if err != nil {
		return []*service.Message{}, err
	}
	newMsg := service.NewMessage(attrsBytes)

	typesBytes, err := json.Marshal(types)
	if err != nil {
		return []*service.Message{}, err
	}
	newMsg.MetaSet("column_types", string(typesBytes))

	return []*service.Message{newMsg}, nil
}

func (js2cols *json2cols) Close(ctx context.Context) error {
	return nil
}
