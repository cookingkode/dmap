package dmap

import (
	"gopkg.in/redis.v4"
	"hash/fnv"
	"strings"
	"time"
)

// Backend  helpers
// -----------------

// Returns shard for    key
func getShard(key string, nItems int) uint {
	hasher := fnv.New32()
	hasher.Write([]byte(key))
	return uint(hasher.Sum32()) % uint(nItems)
}

func (d *Dmap) getBackend(key string) (string, error) {
	return d.rediClients[getShard(key, d.nBackendShards)].Get(key).Result()
}

func (d *Dmap) setBackend(key string, val interface{}, expirySeconds int) error {
	expire := time.Second * (time.Duration(expirySeconds))
	_, err := d.rediClients[getShard(key, d.nBackendShards)].Set(key, val, expire).Result()

	return err
}

func (d *Dmap) delBackend(key string) {
	d.rediClients[getShard(key, d.nBackendShards)].Del(key)
}

func (d *Dmap) watch(cli *redis.Client, notificationChannels []string) {
	//ref : https://godoc.org/gopkg.in/redis.v4#PubSub
	logf("[watcher] %v %v", cli, notificationChannels)
	pubsub, err := cli.Subscribe(notificationChannels...)
	if err != nil {
		panic(err)
	}

	for {
		msg, err := pubsub.ReceiveMessage()
		if err == nil {

			logf("[watcher] %v\n", msg)
			// TODO i hope redis choes not change!!
			offset := strings.Index(msg.Channel, ":")
			if offset <= 0 {
				continue
			}
			event := msg.Channel[offset+1:]

			logf("[watcher] %v event %v %v \n", cli, event, msg.Payload)

			switch event {
			case "set":
				val, err := cli.Get(msg.Payload).Result()
				if err == nil {
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

// local helpers
// -----------------

func (d *Dmap) setLocal(key string, val interface{}) {
	shard := d.shards[getShard(key, d.nShards)]
	shard.Lock()
	defer shard.Unlock()
	shard.theMap[key] = val
}

func (d *Dmap) delLocal(key string) {
	shard := d.shards[getShard(key, d.nShards)]
	shard.Lock()
	defer shard.Unlock()
	delete(shard.theMap, key)
}

func (d *Dmap) getLocal(key string) (interface{}, bool) {
	shard := d.shards[getShard(key, d.nShards)]
	shard.RLock()
	defer shard.RUnlock()

	val, ok := shard.theMap[key]
	return val, ok
}
