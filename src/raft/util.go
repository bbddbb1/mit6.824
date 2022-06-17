package raft

import (
	"log"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	// f, err := os.OpenFile("log.log", os.O_CREATE|os.O_APPEND|os.O_RDWR, os.ModePerm)
	// if err != nil {
	// 	return
	// }
	// defer func() {
	// 	f.Close()
	// }()

	// log.SetOutput(f)

	if Debug {
		log.Printf(format, a...)
	}
	return
}
