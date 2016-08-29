package main

import (
	"flag"
	"fmt"
	"github.com/cookingkode/dmap"
	"github.com/peterh/liner"
	"strings"
)

var (
	flagPort  = flag.Int("port", 9090, "port to listen to")
	flagPeers = flag.String("peers", "", "peers to connect to")
)

func main() {
	flag.Parse()

	dmap := dmap.New("localhost:6379", 32)

	// setup Liner
	line := liner.NewLiner()
	defer line.Close()

	line.SetCtrlCAborts(true)

	fmt.Println("use either , \"set <x> <val>\" or \"get <x>\"")

	for {
		if name, err := line.Prompt(">"); err == nil {
			fmt.Println(name)
			items := strings.Split(name, " ")
			if strings.ToLower(items[0]) == "set" {
				dmap.Set(items[1], items[2])
			} else if strings.ToLower(items[0]) == "get" {
				fmt.Println("VALUE : ", dmap.Get(items[1]))
			} else {
				break
			}
		}
	}
}
