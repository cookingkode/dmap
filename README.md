# Distributed Consistent Map
## In-memory concurrent map which is consistent across machines


## Installation

Install:
	go get github.com/cookingkode/dmap

## Usage

```go
//init : here there are two local redis
myRedisShards := []string{"localhost:6379", "localhost:7777"}
// use DB 0 for all the instances
myDBs := []int{0, 0}
// local shards : function of how much concurreny is there in the app
nShards := 32 

// the instance returned is a thread-safe string-> interface{} map
// values set in other instances are reflected here
dmap := dmap.New(myRedis, myDB, nShards)

// Set without expiry
dmap.Set("Name", "Cooker", 0)
whatIsMyName : = dmap.Get("Name")

// Set with expiry of 2 s
dmap.Set("Foo", "Bar", 2)

dmap.Set("x", 1)


```

## Tests
* see tests/test.go for an interactive shell to test
* TODO : automated tests
