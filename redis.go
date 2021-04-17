package keva

import (
	"time"

	"github.com/go-redis/redis/v8"
)

type redisClientI interface {
	Scan(cursor uint64, match string, count int64) ([]string, uint64, error)
	Set(key string, value interface{}, expiration time.Duration) error
	Get(key string) (string, error)
	Publish(channel string, message interface{}) (int64, error)
	Subscribe(channels ...string) error
	ReceiveMessage() (string, error)
}

type redisClient struct {
	keyPrefix string
	client    *redis.Client
	pubsub    *redis.PubSub
}

func newRedisClient(client *redis.Client) redisClientI {
	return &redisClient{
		client:    client,
		keyPrefix: kevaRedisStoreKeyPrefix,
	}
}

func (r redisClient) Scan(cursor uint64, match string, count int64) ([]string, uint64, error) {
	return r.client.Scan(ctx, cursor, r.keyPrefix+match, count).Result()
}

func (r redisClient) Set(key string, value interface{}, expiration time.Duration) error {
	return r.client.Set(ctx, r.keyPrefix+key, value, expiration).Err()
}

func (r redisClient) Get(key string) (string, error) {
	return r.client.Get(ctx, r.keyPrefix+key).Result()
}

func (r redisClient) Publish(channel string, message interface{}) (int64, error) {
	return r.client.Publish(ctx, channel, message).Result()
}

func (r *redisClient) Subscribe(channels ...string) error {
	pubsub := r.client.Subscribe(ctx)
	err := pubsub.Subscribe(ctx, channels...)
	if err != nil {
		return err
	}
	r.pubsub = pubsub
	return nil
}

func (r redisClient) ReceiveMessage() (string, error) {
	msg, err := r.pubsub.ReceiveMessage(ctx)
	if err != nil {
		return "", err
	}
	return msg.Payload, nil
}
