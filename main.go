package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/laiwei/falcon-index/g"
	"github.com/laiwei/falcon-index/http"
	"github.com/laiwei/falcon-index/index"
)

func start_signal(pid int, cfg *g.GlobalConfig) {
	sigs := make(chan os.Signal, 1)
	log.Println(pid, "register signal notify")
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	for {
		s := <-sigs
		log.Println("recv", s)

		switch s {
		case syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT:
			log.Println("gracefull shut down")
			g.CloseDB()
			log.Println("db closed")
			log.Println(pid, "exit")
			os.Exit(0)
		}
	}
}

func main() {
	cfg := flag.String("c", "cfg.json", "specify config file")
	version := flag.Bool("v", false, "show version")
	versionGit := flag.Bool("vg", false, "show version and git commit log")
	server := flag.Bool("s", false, "run as server or build client tool")
	flag.Parse()

	if *version {
		fmt.Println(g.VERSION)
		os.Exit(0)
	}
	if *versionGit {
		fmt.Println(g.VERSION, g.COMMIT)
		os.Exit(0)
	}

	g.ParseConfig(*cfg)
	if *server {
		//run as server
		g.OpenDB()
		go http.Start()
		start_signal(os.Getpid(), g.Config())
	} else {
		//build index tool
		g.OpenDB()
		defer g.CloseDB()
		index.BuildIndex()
	}

}
