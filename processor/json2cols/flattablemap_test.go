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

func flattenStrMap(t *testing.T, str string) {
	var m map[string]any
	dec := json.NewDecoder(bytes.NewReader([]byte(str)))
	dec.UseNumber()
	dec.Decode(&m)

	fm := flattablemap{}
	t.Log(fm.flatten(m))
}
