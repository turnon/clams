package util

import "strings"

var (
	map_str   = map[string]string{}
	map_int   = map[string]int64{}
	map_float = map[string]float64{}

	nestedInt64Array = []any{
		nil,
		[]*int64{},
		[][]*int64{},
		[][][]*int64{},
		[][][][]*int64{},
		[][][][][]*int64{},
	}

	nestedFloat64Array = []any{
		nil,
		[]*float64{},
		[][]*float64{},
		[][][]*float64{},
		[][][][]*float64{},
		[][][][][]*float64{},
	}

	nestedStrArray = []any{
		nil,
		[]*string{},
		[][]*string{},
		[][][]*string{},
		[][][][]*string{},
		[][][][][]*string{},
	}
)

func PredefinedZeroValue(typeName string) any {
	level := strings.Count(typeName, ")") - 1

	if strings.HasPrefix(typeName, "Map") {
		if strings.HasSuffix(typeName, "String)") {
			return map_str
		} else if strings.HasSuffix(typeName, "Float64)") {
			return map_float
		} else if strings.HasSuffix(typeName, "Int64)") {
			return map_int
		}
		return nil
	}

	// 为什么要两个括号“))”？
	// 因为一层是Array(Nullable(Type))的元素，空值是null
	if strings.HasSuffix(typeName, "Float64))") {
		return nestedFloat64Array[level]
	} else if strings.HasSuffix(typeName, "String))") {
		return nestedStrArray[level]
	} else if strings.HasSuffix(typeName, "Int64))") {
		return nestedInt64Array[level]
	} else {
		return nil
	}
}
