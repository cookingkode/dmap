package dmap

import (
	"fmt"
	"gopkg.in/redis.v4"
	"sync"
)

type Dmap struct {
	//backends
	nBackendShards int
	rediClients    []*redis.Client

	// concurrent map
	nShards int
	shards  []*shard
}

type shard struct {
	sync.RWMutex // ← this mutex protects the map below
	theMap       map[string]interface{}
}

func New(redisBrokers []string, nShards int, db []int) *Dmap {

	handle := new(Dmap)

	// init the concurrent map
	for i := 0; i < nShards; i++ {
		aShard := new(shard)
		aShard.theMap = make(map[string]interface{})
		handle.shards = append(handle.shards, aShard)
	}

	// set the nos
	handle.nShards = nShards
	handle.nBackendShards = len(redisBrokers)

	// init the backends
	for i, broker := range redisBrokers {
		cli := redis.NewClient(&redis.Options{
			Addr:     broker,
			Password: "",    // no password set
			DB:       db[i], // TODO use default DB
		})

		// just listen to Key set event
		// ref ; http://redis.io/topics/notifications
		cli.ConfigSet("notify-keyspace-events", "E$xeg")

		notificationOfInterest := []string{
			fmt.Sprintf("__keyevent@%d__:set", db[i]),
			fmt.Sprintf("__keyevent@%d__:del", db[i]),
			fmt.Sprintf("__keyevent@%d__:expired", db[i]),
		}

		go handle.watch(cli, notificationOfInterest)

		handle.rediClients = append(handle.rediClients, cli)
	}

	return handle
}

func (d *Dmap) Get(key string) interface{} {

	val, present := d.getLocal(key)
	if !present {
		logf("[get] getting from redis..\n")
		val, err := d.getBackend(key)
		if err != nil {
			logf("[get] error %v\n", err)
			return nil
		} else {
			d.setLocal(key, val)
		}
	}

	return val
}

func (d *Dmap) Set(key string, val interface{}, expirySeconds int) {
	d.setLocal(key, val)
	go func() {
		err := d.setBackend(key, val, expirySeconds)
		if err != nil {
			logf("[set] redis set error %v\n", err)
		}
	}()
}

func (d *Dmap) Del(key string) {
	d.delLocal(key)
	go func() {
		// redis client is thread-safe
		d.delBackend(key)
	}()
}
