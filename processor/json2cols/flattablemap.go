package json2cols

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/turnon/clams/util"
)

const (
	tyInt64   = "Int64"
	tyFloat64 = "Float64"
	tyBool    = "UInt8"
	tyString  = "String"
)

type flattablemap struct {
	suffix bool
}

func (fm *flattablemap) suffixed(sf string) string {
	if fm.suffix {
		return sf
	}
	return ""
}

func (fm *flattablemap) flatten(m map[string]any) (attrs map[string]any, types map[string]string) {
	attrs, types = fm.flattenDeep(m, 0)
	return
}

func (fm *flattablemap) flattenDeep(nestedMap map[string]any, level int) (map[string]any, map[string]string) {
	valuesMap := make(map[string]any)
	typesMap := make(map[string]string)
	for k, v := range nestedMap {
		switch realV := v.(type) {
		// 字符串
		case string:
			valuesMap[k] = v
			typesMap[k] = tyString
		case json.Number:
			v, err := realV.Int64()
			if err == nil {
				valuesMap[k] = v
				typesMap[k] = tyInt64
				continue
			}

			valuesMap[k], _ = realV.Float64()
			typesMap[k] = tyFloat64
		// 嵌套object
		case map[string]any:
			subValuesMap, subTypesMap := fm.flattenDeep(realV, level+1)
			for subK, subV := range subValuesMap {
				subTy := subTypesMap[subK]
				if level == 0 {
					subK = fm.suffixTypeOnKey(subK, subTy)
				}
				valuesMap[k+"_"+subK] = subV
				typesMap[k+"_"+subK] = subTy
			}
		// array
		case []any:
			fm.flattenArrayOfAny(valuesMap, typesMap, k, realV)
		case bool:
			if realV {
				valuesMap[k] = uint8(1)
			} else {
				valuesMap[k] = uint8(0)
			}
			typesMap[k] = tyBool
		// 未定
		default:
			// debug
			// ty := reflect.TypeOf(v)
			// if ty != nil {
			// 	fmt.Println(k, ty)
			// } else {
			// 	fmt.Println(k, "nil!")
			// }
			if fmt.Sprintf("%v", v) != "<nil>" {
				valuesMap[k] = v
				typesMap[k] = "unknown_type" + fmt.Sprintf("%v", v)
			}
		}
	}
	// fmt.Println(flatMap, flatTypes)
	return valuesMap, typesMap
}

func (fm *flattablemap) suffixTypeOnKey(key, ty string) string {
	if !fm.suffix {
		return key
	}

	if ty == tyString {
		return key + "_str"
	} else if ty == tyInt64 {
		return key + "_int64"
	} else if ty == tyFloat64 {
		if strings.HasSuffix(key, "_float64") {
			return key
		}
		return key + "_float64"
	} else if ty == tyBool {
		return key + "_bool"
	}
	return key
}

func (fm *flattablemap) flattenArrayOfAny(valuesMap map[string]any, typesMap map[string]string, k string, arrayOfAny []any) {
	if len(arrayOfAny) == 0 {
		return
	}

	var matchType bool

	// string
	_, matchType = arrayOfAny[0].(string)
	if matchType {
		strs := make([]*string, 0, len(arrayOfAny))
		for _, element := range arrayOfAny {
			str := element.(string)
			strs = append(strs, &str)
		}
		suffix := fm.suffixed("_strs")
		valuesMap[k+suffix] = strs
		typesMap[k+suffix] = "Array(Nullable(String))"
		return
	}

	// json.Number
	_, matchType = arrayOfAny[0].(json.Number)
	if matchType {
		var (
			float64s []*float64
			int64s   []*int64
		)
		isInt64 := true
		for _, element := range arrayOfAny {
			i64, err := element.(json.Number).Int64()
			if err != nil {
				isInt64 = false
				break
			}
			if int64s == nil {
				int64s = make([]*int64, 0, len(arrayOfAny))
			}
			int64s = append(int64s, &i64)
		}
		if isInt64 {
			suffix := fm.suffixed("_int64s")
			valuesMap[k+suffix] = int64s
			typesMap[k+suffix] = "Array(Nullable(Int64))"
			return
		}

		float64s = make([]*float64, 0, len(arrayOfAny))
		for _, element := range arrayOfAny {
			f64, _ := element.(json.Number).Float64()
			float64s = append(float64s, &f64)
		}
		suffix := fm.suffixed("_float64s")
		valuesMap[k+suffix] = float64s
		typesMap[k+suffix] = "Array(Nullable(Float64))"
		return
	}

	// bool
	_, matchType = arrayOfAny[0].(bool)
	if matchType {
		uint8s := make([]*uint8, 0, len(arrayOfAny))
		var b uint8
		for _, element := range arrayOfAny {
			if element.(bool) {
				b = uint8(1)
			} else {
				b = uint8(0)
			}
			uint8s = append(uint8s, &b)
		}
		suffix := fm.suffixed("_uint8s")
		valuesMap[k+suffix] = uint8s
		typesMap[k+suffix] = "Array(Nullable(UInt8))"
		return
	}

	// map
	_, matchType = arrayOfAny[0].(map[string]any)
	if matchType {
		maps := make([]map[string]any, 0, len(arrayOfAny))
		for _, element := range arrayOfAny {
			maps = append(maps, element.(map[string]any))
		}
		flatMaps, subFlatTypes := fm.flattenArrayOfMaps(maps)
		for subK, subV := range flatMaps {
			valuesMap[k+"_"+subK] = subV
			typesMap[k+"_"+subK] = subFlatTypes[subK]
		}
		return
	}

	// array
	_, matchType = arrayOfAny[0].([]any)
	if matchType {
		_flatMap := make(map[string]any)
		_flatTypes := make(map[string]string)
		for _, element := range arrayOfAny {
			arr := element.([]any)
			if len(arr) == 0 {
				continue
			}
			fm.flattenArrayOfAny(_flatMap, _flatTypes, "", arr)
			for _, ty := range _flatTypes {
				suffix := fm.suffixed("_arr")
				valuesMap[k+suffix] = arrayOfAny
				typesMap[k+suffix] = "Array(" + ty + ")"
				return
			}
		}
		return
	}

	// for _, eleV := range realV {
	// 	// debug
	// 	ty := reflect.TypeOf(eleV)
	// 	if ty != nil {
	// 		fmt.Println(fmt.Sprintf("reflect %v %v", k, ty))
	// 		// flatMap[k] = v
	// 		// flatTypes[k] = "[]default{}"
	// 	} else {
	// 		fmt.Println(fmt.Sprintf("reflect %v %v", k, "nil!"))
	// 	}
	// }
}

// 键与可能的类型
type keyTypes map[string]map[string]struct{}

// 加入可能的类型
func (keyTys keyTypes) add(k, ty string) {
	tys, ok := keyTys[k]
	if !ok {
		tys = make(map[string]struct{})
	}
	tys[ty] = struct{}{}
	keyTys[k] = tys
}

// 将[{a: 1, b: 2}, {a: 3, b: 4}]变成{a: [1, 3], b: [2, 4]}
func (fm *flattablemap) flattenArrayOfMaps(arrayOfMap []map[string]any) (map[string]any, map[string]string) {
	// 先收集所有会出现的key，以防某些object的key有差异
	keyAndPossibleTypes := make(keyTypes)
	valuesMaps := make([]map[string]any, 0, len(arrayOfMap))
	for _, m := range arrayOfMap {
		subValuesMap, subTypesMap := fm.flatten(m)
		valuesMaps = append(valuesMaps, subValuesMap)
		for k := range subValuesMap {
			keyAndPossibleTypes.add(k, subTypesMap[k])
		}
	}

	// 将各个object中同key的value归类
	keyVals := make(map[string][]any)
	for _, flatMap := range valuesMaps {
		for k := range keyAndPossibleTypes {
			val, ok := flatMap[k]
			if !ok {
				val = util.PredefinedZeroValueForTypes(keyAndPossibleTypes[k])
			}
			keyVals[k] = append(keyVals[k], val)
		}
	}

	// 将[]any转成any
	bytesArr, _ := json.Marshal(keyVals)
	keyVal := make(map[string]any)
	dec := json.NewDecoder(bytes.NewReader(bytesArr))
	dec.UseNumber()
	dec.Decode(&keyVal)

	// 还要将any转为具体类型
	return fm.flatten(keyVal)
}
