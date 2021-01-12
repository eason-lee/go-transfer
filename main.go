package main

import (
	"fmt"
	"go-transfer/config"
	"go-transfer/es"
	"go-transfer/etcd"
	"go-transfer/kafka"
	"io/ioutil"
	"log"

	"gopkg.in/yaml.v2"
)

func initServer(conf *config.ServerConf) {

	// 初始化 etcd
	err := etcd.Init(conf.EtcdConf.Address)
	if err != nil {
		log.Printf("init etcd error : %v \n", err)
		return
	}
	log.Println("etcd 初始化成功")

	// 获取配置
	key := fmt.Sprintf(conf.EtcdKey, conf.Name)
	esConfs, err := etcd.GetTopicEsConf(key)
	if err != nil {
		log.Printf("从 etcd 获取配置失败：%v", err)
	}
	log.Printf("获取 etcd 配置成功 %v\n", esConfs)
	for _, esConf := range esConfs {
		// 初始化 kafka
		kafka.TaskManager.NewConsumerTask(conf.KafkaConf.Address,
			esConf.Topic,
			esConf.Index,
			esConf.Type,
		)
		log.Println("kafka 初始化成功", esConf.Topic)

	}

	//初始化 es
	es.Init(conf.EsConf.URL, conf.EsConf.ChanMaxSize)
}

func main() {
	// 获取配置
	conf := new(config.ServerConf)
	yamlFile, err := ioutil.ReadFile("config/server.yaml")
	if err != nil {
		log.Printf("yamlFile Get err #%v ", err)
	}
	err = yaml.Unmarshal(yamlFile, conf)
	if err != nil {
		log.Fatalf("yaml conf Unmarshal: %v", err)
	}
	// 初始化服务
	initServer(conf)
	// 监听配置修改
	go etcd.WatchConf(fmt.Sprintf(conf.EtcdKey, conf.Name), kafka.TaskManager.UpdateConfChan)
	go kafka.TaskManager.ListenUpdateConf(conf.KafkaConf.Address)
	// 启动 服务
	es.Run(conf.EsConf.SerderNums)

}
