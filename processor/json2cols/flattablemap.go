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
}

func (fm *flattablemap) flatten(m map[string]any) (attrs map[string]any, types map[string]string) {
	attrs, types = fm.flattenDeep(m, 0)
	return
}

func (fm *flattablemap) flattenDeep(nestedMap map[string]any, level int) (map[string]any, map[string]string) {
	flatMap := make(map[string]any)
	flatTypes := make(map[string]string)
	for k, v := range nestedMap {
		switch realV := v.(type) {
		// 字符串
		case string:
			flatMap[k] = v
			flatTypes[k] = tyString
		case json.Number:
			v, err := realV.Int64()
			if err == nil {
				flatMap[k] = v
				flatTypes[k] = tyInt64
				continue
			}

			flatMap[k], _ = realV.Float64()
			flatTypes[k] = tyFloat64
		// 嵌套object
		case map[string]any:
			subMap, subTypes := fm.flattenDeep(realV, level+1)
			for subK, subV := range subMap {
				subTy := subTypes[subK]
				if level == 0 {
					subK = appendTypeToKey(subK, subTy)
				}
				flatMap[k+"_"+subK] = subV
				flatTypes[k+"_"+subK] = subTy
			}
		// array
		case []any:
			fm.flattenArrayOfAny(flatMap, flatTypes, k, realV)
		case bool:
			if realV {
				flatMap[k] = uint8(1)
			} else {
				flatMap[k] = uint8(0)
			}
			flatTypes[k] = tyBool
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
				flatMap[k] = v
				flatTypes[k] = "unknown_type" + fmt.Sprintf("%v", v)
			}
		}
	}
	// fmt.Println(flatMap, flatTypes)
	return flatMap, flatTypes
}

func appendTypeToKey(key, ty string) string {
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

func (fm *flattablemap) flattenArrayOfAny(flatMap map[string]any, flatTypes map[string]string, k string, realV []any) {
	if len(realV) == 0 {
		return
	}

	var matchType bool

	// string
	_, matchType = realV[0].(string)
	if matchType {
		strs := make([]*string, 0, len(realV))
		for _, eleV := range realV {
			str := eleV.(string)
			strs = append(strs, &str)
		}
		flatMap[k+"_strs"] = strs
		flatTypes[k+"_strs"] = "Array(Nullable(String))"
		return
	}

	// json.Number
	_, matchType = realV[0].(json.Number)
	if matchType {
		var (
			float64s []*float64
			int64s   []*int64
		)
		isInt64 := true
		for _, eleV := range realV {
			i64, err := eleV.(json.Number).Int64()
			if err != nil {
				isInt64 = false
				break
			}
			if int64s == nil {
				int64s = make([]*int64, 0, len(realV))
			}
			int64s = append(int64s, &i64)
		}
		if isInt64 {
			flatMap[k+"_int64s"] = int64s
			flatTypes[k+"_int64s"] = "Array(Nullable(Int64))"
			return
		}

		float64s = make([]*float64, 0, len(realV))
		for _, eleV := range realV {
			f64, _ := eleV.(json.Number).Float64()
			float64s = append(float64s, &f64)
		}
		flatMap[k+"_float64s"] = float64s
		flatTypes[k+"_float64s"] = "Array(Nullable(Float64))"
		return
	}

	// bool
	_, matchType = realV[0].(bool)
	if matchType {
		uint8s := make([]*uint8, 0, len(realV))
		var b uint8
		for _, realEleV := range realV {
			if realEleV.(bool) {
				b = uint8(1)
			} else {
				b = uint8(0)
			}
			uint8s = append(uint8s, &b)
		}
		flatMap[k+"_uint8s"] = uint8s
		flatTypes[k+"_uint8s"] = "Array(Nullable(UInt8))"
		return
	}

	// map
	_, matchType = realV[0].(map[string]any)
	if matchType {
		maps := make([]map[string]any, 0, len(realV))
		for _, realEleV := range realV {
			maps = append(maps, realEleV.(map[string]any))
		}
		flatMaps, subFlatTypes := fm.flattenArrayOfMaps(maps)
		for subK, subV := range flatMaps {
			flatMap[k+"_"+subK] = subV
			flatTypes[k+"_"+subK] = subFlatTypes[subK]
		}
		return
	}

	// array
	_, matchType = realV[0].([]any)
	if matchType {
		_flatMap := make(map[string]any)
		_flatTypes := make(map[string]string)
		for _, realEleV := range realV {
			arr := realEleV.([]any)
			if len(arr) == 0 {
				continue
			}
			fm.flattenArrayOfAny(_flatMap, _flatTypes, "", arr)
			for _, ty := range _flatTypes {
				flatMap[k+"_arr"] = realV
				flatTypes[k+"_arr"] = "Array(" + ty + ")"
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

// 将[{a: 1, b: 2}, {a: 3, b: 4}]变成{a: [1, 3], b: [2, 4]}
func (fm *flattablemap) flattenArrayOfMaps(maps []map[string]any) (map[string]any, map[string]string) {
	// 先收集所有会出现的key，以防某些object的key有差异
	keyTys := make(keyTypes)
	flatMaps := make([]map[string]any, 0, len(maps))
	for _, m := range maps {
		flatMap, flatTypes := fm.flatten(m)
		flatMaps = append(flatMaps, flatMap)
		for k := range flatMap {
			keyTys.add(k, flatTypes[k])
		}
	}

	// 将各个object中同key的value归类
	keyVals := make(map[string][]any)
	for _, flatMap := range flatMaps {
		for k := range keyTys {
			val, ok := flatMap[k]
			if !ok {
				val = util.PredefinedZeroValue(keyTys[k])
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
	a, b := fm.flatten(keyVal)
	return a, b
}
