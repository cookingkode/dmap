# Distributed Consistent Map
## In-memory concurrent map which is consistent across machines


## Installation

Install:
	go get github.com/cookingkode/dmap

## Usage

```go
//init
myRedis := "localhost:6379"
myDB := 0 
nShards := 32 // function of how much concurreny is there in the app

// the instance returned is a thread-safe string-> interface{} map
// values set in other instances are reflected here
dmap := dmap.New(myRedis, myDB, nShards)

dmap.Set("Name", "Cooker")
whatIsMyName : = dmap.Get("Name")

dmap.Set("x", 1)


```
