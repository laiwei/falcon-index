package g

import (
	"github.com/boltdb/bolt"
	"log"
	"time"
)

var KVDB *bolt.DB

func OpenDB() {
	path := "./var/index.db"
	var err error
	KVDB, err = bolt.Open(path, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("kvdb:%v\n", KVDB)
}

func CloseDB() {
	if KVDB != nil {
		KVDB.Close()
	}
	log.Printf("db closed")
}
