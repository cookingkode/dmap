package dmap

import (
	"fmt"
	lru "github.com/hashicorp/golang-lru"
	"gopkg.in/redis.v4"
	"sync"
)

type shard struct {
	sync.RWMutex // ← this mutex protects the map below
	theMap       map[string]interface{}
}

type Dmap struct {
	//name
	name string

	//backends
	nBackendShards int
	rediClients    []*redis.Client

	// TwoQueue cache
	twoQueue *lru.TwoQueueCache

	// concurrent map
	nShards int
	shards  []*shard
}

type Config struct {
	RedisBrokers []string
	Db           []int
	NoShards     int
	Name         string
	WantLRU      bool // defaults to False
	NoItemsLRU   int
}

func New(conf *Config) *Dmap {

	handle := new(Dmap)

	if conf.WantLRU {
		//2Q init
		var err error
		handle.twoQueue, err = lru.New2Q(conf.NoItemsLRU)
		if err != nil {
			panic(err)
		}
	} else {
		// init the concurrent map
		for i := 0; i < conf.NoShards; i++ {
			aShard := new(shard)
			aShard.theMap = make(map[string]interface{})
			handle.shards = append(handle.shards, aShard)
		}
	}

	// set the nos
	handle.nShards = conf.NoShards
	handle.nBackendShards = len(conf.RedisBrokers)

	// init the backends
	for i, broker := range conf.RedisBrokers {
		cli := redis.NewClient(&redis.Options{
			Addr:     broker,
			Password: "",         // no password set
			DB:       conf.Db[i], // TODO use default DB
		})

		// just listen to Key set event
		// ref ; http://redis.io/topics/notifications
		cli.ConfigSet("notify-keyspace-events", "E$xeg")

		notificationOfInterest := []string{
			fmt.Sprintf("__keyevent@%d__:set", conf.Db[i]),
			fmt.Sprintf("__keyevent@%d__:del", conf.Db[i]),
			fmt.Sprintf("__keyevent@%d__:expired", conf.Db[i]),
		}

		go handle.watch(cli, notificationOfInterest)

		handle.rediClients = append(handle.rediClients, cli)
	}

	handle.name = conf.Name

	return handle
}

func (d *Dmap) Get(key string) interface{} {

	var err error

	val, present := d.getLocal(key)

	if !present {
		logf("[get] getting from redis..\n")

		val, err = d.getBackend(key)
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
