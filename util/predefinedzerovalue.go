package util

import (
	"strings"
)

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

func PredefinedZeroValue(typeNames map[string]struct{}) any {
	if len(typeNames) == 1 {
		for ty := range typeNames {
			return _predefinedZeroValue(ty)
		}
	}

	if len(typeNames) == 2 {
		var (
			isInt64, isFloat64 bool
			float64Ty          string
		)
		for ty := range typeNames {
			if strings.Contains(ty, "Int64") {
				isInt64 = true
			}
			if strings.Contains(ty, "Float64") {
				isFloat64 = true
				float64Ty = ty
			}
		}
		if isInt64 && isFloat64 {
			return _predefinedZeroValue(float64Ty)
		}
	}

	return nil
}

func _predefinedZeroValue(typeName string) any {
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

	level := strings.Count(typeName, ")") - 1

	// 如果非数组
	if level == -1 {
		if typeName == "Float64" || typeName == "Int64" || typeName == "UInt8" {
			return 0
		} else if typeName == "String" {
			return ""
		} else {
			return nil
		}
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
