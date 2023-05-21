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
		Field(service.NewObjectListField(
			"fields",
			service.NewStringField("name"),
			service.NewBoolField("keep").Default(false),
		))

	constructor := func(conf *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
		fields, err := conf.FieldObjectList("fields")
		if err != nil {
			return nil, err
		}
		jsonField2colArr := make([]jsonField2col, 0, len(fields))
		for _, f := range fields {
			name, err := f.FieldString("name")
			if err != nil {
				return nil, err
			}
			keep, err := f.FieldBool("keep")
			if err != nil {
				return nil, err
			}
			jsonField2colArr = append(jsonField2colArr, jsonField2col{name: name, keep: keep})
		}
		return &json2cols{fields: jsonField2colArr}, nil
	}

	err := service.RegisterProcessor("json2cols", configSpec, constructor)
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

type json2cols struct {
	fields []jsonField2col
}

type jsonField2col struct {
	name string
	keep bool
}

func (js2cols *json2cols) Process(ctx context.Context, msg *service.Message) (service.MessageBatch, error) {
	structed, err := msg.AsStructured()
	if err != nil {
		return nil, err
	}

	// try to destruct those values to map or slice
	msgAsMap := structed.(map[string]any)
	for _, fieldToChange := range js2cols.fields {
		js2cols.destructValue(msgAsMap, fieldToChange.name, fieldToChange.keep)
	}

	// flatten and get types
	attrs, types := flattenMap(msgAsMap)

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

func (js2cols *json2cols) destructValue(msgAsMap map[string]any, field string, keep bool) {
	value := msgAsMap[field]
	if keep {
		msgAsMap[field+"_raw"] = value
	}

	var destructedValue any
	if value != nil && reflect.TypeOf(value).Name() == "string" {
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

	msgAsMap[field] = destructedValue
}

func (js2cols *json2cols) Close(ctx context.Context) error {
	return nil
}
