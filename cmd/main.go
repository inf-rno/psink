package main

import (
	"github.com/inf-rno/psink/pkg/psync"
)

func main() {
	psync.New("localhost:6379", "localhost:6380").Go()
}
