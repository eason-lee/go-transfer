package main

import (
	_ "go-transfer/kafka"

	"go-transfer/es"
)

func initServer() {
	// 初始化 etcd
	// err := etcd.Init(config.Conf.EtcdConf.Address)
	// if err != nil {
	// 	log.Printf("init etcd error : %v \n", err)
	// 	return
	// }
	// log.Println("etcd 初始化成功")

	//初始化 es
	// es.Init()

	// kafka.Init()

	// 监听配置修改
	// go etcd.WatchConf(fmt.Sprintf(config.Conf.EtcdKey, config.Conf.Name), kafka.TaskManager.UpdateConfChan)
	// go kafka.TaskManager.ListenUpdateConf(config.Conf.KafkaConf.Address)

}

func main() {
	// 初始化服务
	// initServer()

	// 启动 服务
	es.Run()
}
