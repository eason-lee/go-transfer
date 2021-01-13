package main

import (
	_ "go-transfer/kafka"

	"go-transfer/es"
)

func main() {
	es.Run()
}
