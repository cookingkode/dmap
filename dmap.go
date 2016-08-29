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

func New(redisBroker string, nShards int) *Dmap {
	if redisBroker == "" {
		redisBroker = "localhost:6379" // default
	}
	handle := new(Dmap)

	handle.rediCli = redis.NewClient(&redis.Options{
		Addr:     redisBroker,
		Password: "", // no password set
		DB:       0,  // TODO use default DB
	})

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
	shard := d.getShard(key)
	shard.Lock()
	defer shard.Unlock()
	shard.theMap[key] = val
	go func() {
		// redis client is thread-safe
		err := d.rediCli.Set(key, val, 0) // TODO check error etc
		fmt.Println("[set] redis error ", err)
	}()
}

// Returns shard under given key
func (d *Dmap) getShard(key string) *shard {
	hasher := fnv.New32()
	hasher.Write([]byte(key))
	return d.shards[uint(hasher.Sum32())%uint(d.nShards)]
}
