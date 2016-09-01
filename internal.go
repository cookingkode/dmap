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

func (d *Dmap) getBackendKey(key string) string {
	// prefix
	return d.name + key
}

func (d *Dmap) getLocalKey(key string) string {
	// prefix
	return strings.TrimPrefix(key, d.name)

}

func (d *Dmap) getBackend(key string) (string, error) {
	key = d.getBackendKey(key)
	return d.rediClients[getShard(key, d.nBackendShards)].Get(key).Result()
}

func (d *Dmap) setBackend(key string, val interface{}, expirySeconds int) error {
	key = d.getBackendKey(key)
	expire := time.Second * (time.Duration(expirySeconds))
	_, err := d.rediClients[getShard(key, d.nBackendShards)].Set(key, val, expire).Result()

	return err
}

func (d *Dmap) delBackend(key string) {
	key = d.getBackendKey(key)
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

			key := d.getLocalKey(msg.Payload)
			logf("[watcher] %v event %v %v  KEY = %v \n", cli, event, msg.Payload, key)

			switch event {
			case "set":
				val, err := cli.Get(msg.Payload).Result()
				if err == nil {
					d.SetLocal(key, val)
				}
			case "del":
				d.DelLocal(key)
			case "expired":
				d.DelLocal(key)
			}

		}

	}

}

// local helpers
// -----------------

// SetLocal sets a value only locally
func (d *Dmap) SetLocal(key string, val interface{}) {
	if d.twoQueue != nil {
		d.twoQueue.Add(key, val)
	} else {
		shard := d.shards[getShard(key, d.nShards)]
		shard.Lock()
		defer shard.Unlock()
		shard.theMap[key] = val
	}
}

// DelLocal deletes a value only locally
func (d *Dmap) DelLocal(key string) {
	if d.twoQueue != nil {
		d.twoQueue.Remove(key)
	} else {
		shard := d.shards[getShard(key, d.nShards)]
		shard.Lock()
		defer shard.Unlock()
		delete(shard.theMap, key)
	}
}

// SetLocal gets a value from local only
func (d *Dmap) GetLocal(key string) (interface{}, bool) {
	var (
		val interface{}
		ok  bool
	)

	if d.twoQueue != nil {
		val, ok = d.twoQueue.Get(key)
	} else {
		shard := d.shards[getShard(key, d.nShards)]
		shard.RLock()
		defer shard.RUnlock()
		val, ok = shard.theMap[key]
	}

	return val, ok
}
