package keva

import "context"

var (
	ctx context.Context = context.TODO()
)

const (
	kevaRedisStoreKeyPrefix string = "{keva_store}:"
	bitcaskDBFilePrefix     string = "/tmp/"

	colonSeparator string = ":"

	channelBufferSize int = 100
)
