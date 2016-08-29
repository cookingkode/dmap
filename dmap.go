package dmap

import (
	"fmt"
	"gopkg.in/redis.v4"
	"hash/fnv"
	"sync"
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
	handle.rediCli.ConfigSet("notify-keyspace-events", "E$")
	listeningChannel := fmt.Sprintf("__keyevent@%d__:set", db)

	go handle.watch(listeningChannel)

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

func (d *Dmap) Set(key string, val interface{}) {
	d.setLocal(key, val)
	go func() {
		// redis client is thread-safe
		err := d.rediCli.Set(key, val, 0) // TODO check error etc
		fmt.Println("[set] redis error ", err)
	}()
}

//set locally
func (d *Dmap) setLocal(key string, val interface{}) {
	shard := d.getShard(key)
	shard.Lock()
	defer shard.Unlock()
	shard.theMap[key] = val
}

// Returns shard under given key
func (d *Dmap) getShard(key string) *shard {
	hasher := fnv.New32()
	hasher.Write([]byte(key))
	return d.shards[uint(hasher.Sum32())%uint(d.nShards)]
}

func (d *Dmap) watch(notificationChannel string) {
	//ref : https://godoc.org/gopkg.in/redis.v4#PubSub
	pubsub, err := d.rediCli.Subscribe(notificationChannel)
	if err != nil {
		panic(err)
	}

	fmt.Println("[watcher] listening to ", notificationChannel)
	for {
		msg, err := pubsub.ReceiveMessage()
		if err == nil {
			// fmt.Println("[watcher]", msg.Payload, "is set..")
			val, err := d.rediCli.Get(msg.Payload).Result()
			if err == nil {
				//fmt.Println("[watcher] setting ", msg.Payload, " to ", val)
				d.setLocal(msg.Payload, val)
			}
		}

	}

}
