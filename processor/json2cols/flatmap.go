package json2cols

import (
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

func flattenMap(nestedMap map[string]any) (attrs map[string]any, types map[string]string) {
	// defer func() {
	// 	if err := recover(); err != nil {
	// 		logger.Debugf("FlattenMap_Err: %v ==> %v", err, nestedMap)
	// 		panic(err)
	// 	}
	// }()

	attrs, types = flattenDeepMap(nestedMap, 0)
	return
}

func flattenDeepMap(nestedMap map[string]any, level int) (map[string]any, map[string]string) {
	flatMap := make(map[string]any)
	flatTypes := make(map[string]string)
	for k, v := range nestedMap {
		switch realV := v.(type) {
		// 字符串
		case string:
			flatMap[k] = v
			flatTypes[k] = tyString
		// 嵌套object
		case map[string]any:
			subMap, subTypes := flattenDeepMap(realV, level+1)
			for subK, subV := range subMap {
				subTy := subTypes[subK]
				if level == 0 {
					subK = appendTypeToKey(subK, subTy)
				}
				flatMap[k+"_"+subK] = subV
				flatTypes[k+"_"+subK] = subTy
			}
		// 嵌套array
		case []any:
			maps := make([]map[string]any, 0, len(realV))
			isMaps := false
			var (
				float64s []*float64
				strs     []*string
				int64s   []*int64
			)
			for _, eleV := range realV {
				switch realEleV := eleV.(type) {
				case map[string]any:
					maps = append(maps, realEleV)
					isMaps = true
				case float64:
					if float64s == nil {
						float64s = make([]*float64, 0, len(realV))
					}
					float64s = append(float64s, &realEleV)
					flatMap[k+"_float64s"] = float64s
					flatTypes[k+"_float64s"] = "Array(Nullable(Float64))"
				case string:
					if strs == nil {
						strs = make([]*string, 0, len(realV))
					}
					strs = append(strs, &realEleV)
					flatMap[k+"_strs"] = strs
					flatTypes[k+"_strs"] = "Array(Nullable(String))"
				case int64:
					if int64s == nil {
						int64s = make([]*int64, 0, len(realV))
					}
					int64s = append(int64s, &realEleV)
					flatMap[k+"_int64s"] = int64s
					flatTypes[k+"_int64s"] = "Array(Nullable(Int64))"
				default:
					// debug
					// ty := reflect.TypeOf(eleV)
					// if ty != nil {
					// 	logger.Debug(fmt.Sprintf("reflect %v %v", k, ty))
					// 	// flatMap[k] = v
					// 	// flatTypes[k] = "[]default{}"
					// } else {
					// 	logger.Debug(fmt.Sprintf("reflect %v %v", k, "nil!"))
					// }
				}
				// if !isMaps {
				// 	break
				// }
			}
			if !isMaps {
				continue
			}
			flatMaps, subFlatTypes := flattenMaps(maps)
			for subK, subV := range flatMaps {
				flatMap[k+"_"+subK] = subV
				flatTypes[k+"_"+subK] = subFlatTypes[subK]
			}
		case int:
			flatMap[k] = int64(realV)
			flatTypes[k] = tyInt64
		case float64:
			// flatMap[k+"_float64"] = realV
			// flatTypes[k+"_float64"] = tyFloat64
			flatMap[k] = realV
			flatTypes[k] = tyFloat64
		case json.Number:
			flatMap[k], _ = realV.Float64()
			flatTypes[k] = tyFloat64
		case int64:
			flatMap[k] = realV
			flatTypes[k] = tyInt64
		case bool:
			if realV {
				flatMap[k] = uint8(1)
			} else {
				flatMap[k] = uint8(0)
			}
			flatTypes[k] = tyBool
		case []string:
			flatMap[k+"_strs"] = realV
			flatTypes[k+"_strs"] = "Array(Nullable(String))"
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

// 将[{a: 1, b: 2}, {a: 3, b: 4}]变成{a: [1, 3], b: [2, 4]}
func flattenMaps(maps []map[string]any) (map[string]any, map[string]string) {
	// 先收集所有会出现的key，以防某些object的key有差异
	KeyTypes := make(map[string]string)
	flatMaps := make([]map[string]any, 0, len(maps))
	for _, m := range maps {
		flatMap, flatTypes := flattenMap(m)
		flatMaps = append(flatMaps, flatMap)
		for k := range flatMap {
			if _, ok := KeyTypes[k]; ok {
				continue
			}
			ty := flatTypes[k]
			if strings.HasSuffix(ty, ")") {
				KeyTypes[k] = "Array(" + flatTypes[k] + ")"
			} else {
				KeyTypes[k] = "Array(Nullable(" + flatTypes[k] + "))"
			}
		}
	}

	// 将各个object中同key的value归类
	keyVals := make(map[string][]any)
	for _, flatMap := range flatMaps {
		for k := range KeyTypes {
			val, ok := flatMap[k]
			if !ok {
				val = util.PredefinedZeroValue(KeyTypes[k])
			}
			keyVals[k] = append(keyVals[k], val)
		}
	}

	// 将[]any转成any
	bytes, _ := json.Marshal(keyVals)
	keyVal := make(map[string]any)
	json.Unmarshal(bytes, &keyVal)

	// 还要将any转为具体类型
	return flattenMap(keyVal)
}
