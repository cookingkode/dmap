# Distributed Consistent Map
## In-memory concurrent map which is consistent across machines


## Installation

Install:
	go get github.com/cookingkode/dmap

## Usage

```go
// Prep the config
conf := &dmap.Config{
	[]string{"localhost:6379", "localhost:7777"}, // your redis hosts
	[]int{0, 0}, // DBs of each of the redis hosts
	32, // local shards : function of how much concurreny is there in the app
	"test", // Dmap name , used as a namespace for backend
	true, // Want 2Q based LRU?
	4, // Valid if LRU, no of items in the LRU cache
}

//init 
// the instance returned is a thread-safe string-> interface{} map
// values set in other instances are reflected here
dmap := dmap.New(conf)

// Set without expiry
dmap.Set("Name", "Cooker", 0)
whatIsMyName : = dmap.Get("Name")

// Set with expiry of 2 s
dmap.Set("Foo", "Bar", 2)

dmap.Set("x", 1)


```

## Tests
* see tests/test.go for an interactive shell to test normal map
* see tests/test_lru.go for an interactive shell to test normal map
* TODO : automated tests
