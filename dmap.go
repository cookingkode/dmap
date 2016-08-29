package dmap

import (
	"fmt"
	"gopkg.in/redis.v4"
	"hash/fnv"
	"strings"
	"sync"
	"time"
)

type Dmap struct {
	//config part
	rediCli *redis.Client
	nShards int

	// concurrent map
	shards []*shard
}

type shard struct {
	sync.Mutex // ← this mutex protects the map below
	theMap     map[string]interface{}
}

func New(redisBroker string, nShards int, db int) *Dmap {
	if redisBroker == "" {
		redisBroker = "localhost:6379" // default
	}
	handle := new(Dmap)

	handle.rediCli = redis.NewClient(&redis.Options{
		Addr:     redisBroker,
		Password: "", // no password set
		DB:       db, // TODO use default DB
	})

	// just listen to Key set event
	// ref ; http://redis.io/topics/notifications
	handle.rediCli.ConfigSet("notify-keyspace-events", "E$xeg")

	notificationOfInterest := []string{
		fmt.Sprintf("__keyevent@%d__:set", db),
		fmt.Sprintf("__keyevent@%d__:del", db),
		fmt.Sprintf("__keyevent@%d__:expired", db),
	}

	go handle.watch(notificationOfInterest)

	//init the concurrent map
	for i := 0; i < nShards; i++ {
		aShard := new(shard)
		aShard.theMap = make(map[string]interface{})
		handle.shards = append(handle.shards, aShard)
	}
	handle.nShards = nShards

	return handle
}

func (d *Dmap) Get(key string) interface{} {
	shard := d.getShard(key)
	shard.Lock()
	defer shard.Unlock()

	var err error
	var val interface{}
	val, present := shard.theMap[key]
	if !present {
		fmt.Println("[get] getting from redis..")
		val, err = d.rediCli.Get(key).Result()
		if err != nil {
			fmt.Println("[get]", err)
			return nil
		}
		shard.theMap[key] = val
	}

	return val
}

func (d *Dmap) Set(key string, val interface{}, expirySeconds int) {
	d.setLocal(key, val)
	go func() {

		expire := time.Second * (time.Duration(expirySeconds))
		// redis client is thread-safe
		err := d.rediCli.Set(key, val, expire) // TODO check error etc
		fmt.Println("[set] redis set error ", err)
	}()
}

func (d *Dmap) Del(key string) {
	d.delLocal(key)
	go func() {
		// redis client is thread-safe
		err := d.rediCli.Del(key)
		fmt.Println("[set] redis del error ", err)
	}()
}

/****************************** local *************************************/

//set locally
func (d *Dmap) setLocal(key string, val interface{}) {
	shard := d.getShard(key)
	shard.Lock()
	defer shard.Unlock()
	shard.theMap[key] = val
}

//set locally
func (d *Dmap) delLocal(key string) {
	shard := d.getShard(key)
	shard.Lock()
	defer shard.Unlock()
	delete(shard.theMap, key)
}

// Returns shard under given key
func (d *Dmap) getShard(key string) *shard {
	hasher := fnv.New32()
	hasher.Write([]byte(key))
	return d.shards[uint(hasher.Sum32())%uint(d.nShards)]
}

func (d *Dmap) watch(notificationChannels []string) {
	//ref : https://godoc.org/gopkg.in/redis.v4#PubSub
	pubsub, err := d.rediCli.Subscribe(notificationChannels...)
	if err != nil {
		panic(err)
	}

	for {
		msg, err := pubsub.ReceiveMessage()
		if err == nil {
			// TODO i hope redis choes not change!!
			offset := strings.Index(msg.Channel, ":")
			if offset <= 0 {
				continue
			}
			event := msg.Channel[offset+1:]

			//fmt.Println("[watcher] event ", event, msg.Payload)

			switch event {
			case "set":
				val, err := d.rediCli.Get(msg.Payload).Result()
				if err == nil {
					//fmt.Println("[watcher] setting ", msg.Payload, " to ", val)
					d.setLocal(msg.Payload, val)
				}
			case "del":
				d.delLocal(msg.Payload)
			case "expired":
				d.delLocal(msg.Payload)
			}

		}

	}

}
