package util

import "gopkg.in/yaml.v3"

func InterpolateYamlAnchor(anchors string, ymlStr string) (string, error) {
	if anchors == "" {
		return ymlStr, nil
	}

	var mergedYmlMap map[string]any
	if err := yaml.Unmarshal([]byte(anchors+"\n"+ymlStr), &mergedYmlMap); err != nil {
		return "", err
	}
	var anchorsMap map[string]any
	if err := yaml.Unmarshal([]byte(anchors), &anchorsMap); err != nil {
		return "", err
	}
	for anchorsKey := range anchorsMap {
		delete(mergedYmlMap, anchorsKey)
	}

	mergedYmlBytes, err := yaml.Marshal(mergedYmlMap)
	if err != nil {
		return "", err
	}

	return string(mergedYmlBytes), nil
}
