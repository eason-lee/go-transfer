package main

import (
	"go-transfer/config"
	"go-transfer/kafka"
	"log"

	"go-transfer/es"
	"go-transfer/filter"
	"go-transfer/handler"
	"go-transfer/service"
)

func main() {
	conf := config.GetConf("config/config.yaml")

	group := service.NewServiceGroup()
	defer group.Stop()

	filters := filter.CreateFilters(conf)
	writer, err := es.NewWriter(&conf.EsConf)

	if err != nil {
		log.Fatalf("NewWriter err #%v ", err)
	}

	handle := handler.NewHandler(&conf.EsConf, writer)
	handle.AddFilters(filters...)

	// add kafka server
	for _, k := range config.GetKqConf(&conf.KafkaConf) {
		group.Add(kafka.NewQueue(&k, handle))
	}

	log.Println("service start")
	group.Start()

}
