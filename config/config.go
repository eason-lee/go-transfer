package config

import (
	"io/ioutil"
	"log"

	"gopkg.in/yaml.v2"
)

// ServerConf 服务配置
type ServerConf struct {
	Name      string `yaml:"server_name"`
	KafkaConf `yaml:"kafka"`
	EtcdConf  `yaml:"etcd"`
	EsConf    `yaml:"elasticsearch"`
}

// KafkaConf ...
type KafkaConf struct {
	Address []string `yaml:"address,flow"`
}

// EtcdConf ...
type EtcdConf struct {
	Address []string `yaml:"address,flow"`
}

// EsConf ...
type EsConf struct {
	URL         string `yaml:"url"`
	EtcdKey     string `yaml:"etcd_key"`
	ChanMaxSize int    `yaml:"chan_max_size"`
	SerderNums  int    `yaml:"sender_nums"`
}

// Conf 配置
var Conf = new(ServerConf)

// Init 初始化配置
func init() {
	yamlFile, err := ioutil.ReadFile("config/server.yaml")
	if err != nil {
		log.Printf("yamlFile Get err #%v ", err)
	}
	err = yaml.Unmarshal(yamlFile, Conf)
	if err != nil {
		log.Fatalf("yaml conf Unmarshal: %v", err)
	}
}
