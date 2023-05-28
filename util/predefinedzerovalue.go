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

// 可能的类型列表
type PossibleTypes map[string]struct{}

// 从可能的类型列表中推断出零值
func (posTys PossibleTypes) PredefinedZeroValue() any {
	return PredefinedZeroValue(posTys.possibleType())
}

// 从可能的类型列表中推断出一种类型
func (posTys PossibleTypes) possibleType() string {
	if len(posTys) == 1 {
		for ty := range posTys {
			return ty
		}
	}

	if len(posTys) == 2 {
		var (
			isInt64, isFloat64 bool
			float64Ty          string
		)
		for ty := range posTys {
			if strings.Contains(ty, "Int64") {
				isInt64 = true
			}
			if strings.Contains(ty, "Float64") {
				isFloat64 = true
				float64Ty = ty
			}
		}
		if isInt64 && isFloat64 {
			return float64Ty
		}
	}

	return ""
}

func PredefinedZeroValue(typeName string) any {
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
		if typeName == "Float64" {
			return float64(0)
		} else if typeName == "Int64" {
			return int64(0)
		} else if typeName == "UInt8" {
			return uint8(0)
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
