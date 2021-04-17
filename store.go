package keva

import (
	"log"

	"github.com/go-redis/redis/v8"
	"github.com/prologic/bitcask"
)

type Store struct {
	db           *bitcask.Bitcask
	redisClient  redisClientI
	buffer       chan string
	initDone     chan bool
	bufferSize   int64
	redisChannel string
}

func NewStore(clientId, redisAddr, redisPassword, redisChannel string, redisDB int, bufferSize int64) *Store {
	redisClient := redis.NewClient(&redis.Options{
		Addr:     redisAddr,
		Password: redisPassword,
		DB:       redisDB,
	})
	client := newRedisClient(redisClient)
	bufferChan := make(chan string, channelBufferSize)
	db, err := bitcask.Open(bitcaskDBFilePrefix + clientId)
	if err != nil {
		log.Fatal("Failed to open db " + err.Error())
	}
	store := &Store{
		db:           db,
		redisClient:  client,
		buffer:       bufferChan,
		initDone:     make(chan bool),
		bufferSize:   bufferSize,
		redisChannel: redisChannel,
	}
	go store.loadData()
	go store.subscribe()
	return store
}

func (k Store) Get(key string) string {
	val, err := k.get(key)
	if err != nil {
		log.Printf("Failed to get message " + err.Error())
	}
	return string(val)
}

func (k Store) Put(key string, value string) {
	k.put(key, value)
	defer k.broadcastAndPersist(key, value)
}

func (k Store) loadData() {
	var cursor uint64
	for {
		keys, cursor, err := k.redisClient.Scan(cursor, "*", k.bufferSize)
		if err != nil {
			log.Println("Failed to retrieve data ", err.Error())
		}
		for _, key := range keys {
			val, _ := k.redisClient.Get(key)
			k.put(key, val)
		}
		if cursor == 0 {
			break
		}
	}
	k.initDone <- true
}

func (k Store) put(key string, value string) error {
	err := k.db.Put([]byte(kevaRedisStoreKeyPrefix+key), []byte(value))
	if err != nil {
		log.Printf("Failed to write message " + err.Error())
		return err
	}
	return nil
}

func (k Store) get(key string) (string, error) {
	value, err := k.db.Get([]byte(kevaRedisStoreKeyPrefix + key))
	return string(value), err
}

func (k Store) broadcastAndPersist(key string, value string) {
	publish_message := key + colonSeparator + value
	k.redisClient.Publish(k.redisChannel, publish_message)
	err := k.redisClient.Set(key, value, 0)
	if err != nil {
		log.Println("Failed to persist " + err.Error())
	}
}

func (k Store) subscribe() {
	if err := k.redisClient.Subscribe(k.redisChannel); err != nil {
		log.Fatal("Failed to subscribe " + err.Error())
	}

	go func() {
		// wait for initlization to complete before processing new events
		<-k.initDone
		for {
			msg := <-k.buffer
			key, value := payloadToKeyValue(msg)
			k.put(key, value)
		}
	}()
	for {
		msg, err := k.redisClient.ReceiveMessage()
		if err != nil {
			log.Printf("Error " + err.Error())
		}
		// if we're still at inital load, buffer messages
		k.buffer <- msg
	}
}
