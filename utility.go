package keva

import "strings"

func payloadToKeyValue(payload string) (key, value string) {
	parts := strings.Split(payload, colonSeparator)
	key = strings.Join(parts[:len(parts)-1], colonSeparator)
	value = parts[len(parts)-1:][0]
	return
}
