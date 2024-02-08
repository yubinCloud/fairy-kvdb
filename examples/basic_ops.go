package main

import (
	fairydb "fairy-kvdb"
	"fmt"
)

func main() {
	options := fairydb.DefaultOptions
	db, err := fairydb.Open(options)
	if err != nil {
		panic(err)
	}

	err = db.Put([]byte("name"), []byte("zhangSan"))
	if err != nil {
		panic(err)
	}
	val, err := db.Get([]byte("name"))
	if err != nil {
		panic(err)
	}
	fmt.Println("val = ", string(val))

	err = db.Delete([]byte("name"))
	if err != nil {
		panic(err)
	}

	val, err = db.Get([]byte("name"))
	if err != nil {
		panic(err)
	}
	fmt.Println("val = ", string(val))
}
