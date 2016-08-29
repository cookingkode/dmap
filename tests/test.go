package main

import (
	"flag"
	"fmt"
	"github.com/cookingkode/dmap"
	"github.com/peterh/liner"
	"log"
	"os"
	"strconv"
	"strings"
)

var (
	flagPort  = flag.Int("port", 9090, "port to listen to")
	flagPeers = flag.String("peers", "", "peers to connect to")
)

func main() {

	flag.Parse()

	dmap.Logger = log.New(os.Stderr, "", log.LstdFlags)

	// uncomment this in case you need logs
	dmap.Logger = nil

	dmap := dmap.New("localhost:6379", 32, 0)

	// setup Liner
	line := liner.NewLiner()
	defer line.Close()

	line.SetCtrlCAborts(true)

	fmt.Println("Usage :")
	fmt.Println("SET: setx <x> <val> <second_expiry> : <second_expiry> is optiional")
	fmt.Println("GET  : get <x> ")
	fmt.Println("DEL  : del <x> ")

	for {
		if name, err := line.Prompt(">"); err == nil {
			fmt.Println(name)
			items := strings.Split(name, " ")
			if strings.ToLower(items[0]) == "set" {
				expiry := 0
				if len(items) == 3 {
					expiry = 0
				} else {
					expiry, _ = strconv.Atoi(items[3])
				}

				dmap.Set(items[1], items[2], expiry)

			} else if strings.ToLower(items[0]) == "get" {
				fmt.Println("VALUE : ", dmap.Get(items[1]))
			} else if strings.ToLower(items[0]) == "del" {
				dmap.Del(items[1])
			} else {
				break
			}
		}
	}
}
