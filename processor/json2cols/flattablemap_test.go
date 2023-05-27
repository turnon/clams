package json2cols

import (
	"bytes"
	"encoding/json"
	"testing"
)

func TestFlattablemapInt(t *testing.T) {
	flattenStrMap(t, "{\"a\":1}")
}

func TestFlattenMapFloat(t *testing.T) {
	flattenStrMap(t, "{\"a\":1.2}")
}

func TestFlattenMapString(t *testing.T) {
	flattenStrMap(t, "{\"a\":\"b\"}")
}

func TestFlattenMapEmptyArr(t *testing.T) {
	flattenStrMap(t, "{\"a\":[]}")
}

func TestFlattenMapBoolArr(t *testing.T) {
	flattenStrMap(t, "{\"a\":[true, false]}")
}

func TestFlattenMapIntArr(t *testing.T) {
	flattenStrMap(t, "{\"a\":[1, 2]}")
}

func TestFlattenMapFloatArr(t *testing.T) {
	flattenStrMap(t, "{\"a\":[1.2, 3.4]}")
}

func TestFlattenMapStringArr(t *testing.T) {
	flattenStrMap(t, "{\"a\":[\"b\", \"c\"]}")
}

func TestFlattenMapObjArr(t *testing.T) {
	flattenStrMap(t, "{\"a\": [{\"b\": 1}, {\"b\": 2, \"c\": 3.4, \"d\": true, \"e\": [5, 6]}]}")
}

func TestFlattenMapNested(t *testing.T) {
	flattenStrMap(t, "{\"a\": {\"b\": 1, \"c\": 2.3, \"d\": \"d\", \"e\": [4, 5]}}")
}

func flattenStrMap(t *testing.T, str string) {
	const format = "\n\033[32mnew: %v\n\033[33mnew: %v\n\033[34mattr: %v\n\033[35mtype: %v\n\033[0m"

	var m map[string]any
	dec := json.NewDecoder(bytes.NewReader([]byte(str)))
	dec.UseNumber()
	dec.Decode(&m)

	fm := flattablemap{}
	val, ty := fm.flatten(m)
	valStr, _ := json.Marshal(val)
	t.Logf(format, str, string(valStr), val, ty)
}
