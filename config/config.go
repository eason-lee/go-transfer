package config

import (
	"log"

	"github.com/spf13/viper"
)

// ServerConf 服务配置
type ServerConf struct {
	Name      string `mapstructure:"server_name"`
	KafkaConf `mapstructure:"kafka"`
	EtcdConf  `mapstructure:"etcd"`
	EsConf    `mapstructure:"elasticsearch"`
	Filters   []FilterConf `mapstructure:"filters,optional"`
}

// KafkaConf ...
type KafkaConf struct {
	Brokers    []string `mapstructure:"brokers"`
	Group      string   `mapstructure:"group"`
	Topics     []string `mapstructure:"topics"`
	Offset     string   `mapstructure:"offset"`
	Conns      int      `mapstructure:"conns"`
	Consumers  int      `mapstructure:"consumers"`
	Processors int      `mapstructure:"processors"`
	MinBytes   int      `mapstructure:"min_bytes"`
	MaxBytes   int      `mapstructure:"max_bytes"`
}

// KqConf ...
type KqConf struct {
	Brokers    []string
	Group      string
	Topic      string
	Offset     string
	Conns      int
	Consumers  int
	Processors int
	MinBytes   int
	MaxBytes   int
}

// EtcdConf ...
type EtcdConf struct {
	Address []string `mapstructure:"address,flow"`
}

// EsConf ...
type EsConf struct {
	Hosts         []string `mapstructure:"hosts"`
	Compress      bool     `mapstructure:"compress"`
	Index         string   `mapstructure:"index"`
	DocType       string   `mapstructure:"doc_type"`
	MaxChunkBytes int      `mapstructure:"max_chunk_bytes"` // default 15M
}

// FilterConf ...
type FilterConf struct {
	Action string   `mapstructure:"action"`
	Fields []string `mapstructure:"fields"`
	Field  string   `mapstructure:"field"`
	Target string   `mapstructure:"target"`
}

// GetKqConf 获取 kafka 配置
func GetKqConf(c *KafkaConf) []KqConf {
	var ret []KqConf

	for _, topic := range c.Topics {
		ret = append(ret, KqConf{
			Brokers:    c.Brokers,
			Group:      c.Group,
			Topic:      topic,
			Offset:     c.Offset,
			Conns:      c.Conns,
			Consumers:  c.Consumers,
			Processors: c.Processors,
			MinBytes:   c.MinBytes,
			MaxBytes:   c.MaxBytes,
		})
	}

	return ret
}

// GetConf ...
func GetConf(configName string) *ServerConf {
	conf := viper.New()
	conf.SetConfigFile(configName)

	// 设置默认值
	conf.SetDefault("kafka.offset", "first")
	conf.SetDefault("kafka.conns", 2)
	conf.SetDefault("kafka.consumers", 2)
	conf.SetDefault("kafka.processors", 2)
	conf.SetDefault("kafka.min_bytes", 1048576)
	conf.SetDefault("kafka.max_bytes", 10485760)

	conf.SetDefault("elasticsearch.compress", false)
	conf.SetDefault("elasticsearch.doc_type", "_doc")
	conf.SetDefault("elasticsearch.max_chunk_bytes", 1024*1024)

	err := conf.ReadInConfig() // 查找并读取配置文件
	if err != nil {            // 处理读取配置文件的错误
		log.Fatalf("mapstructureFile Get err #%v ", err)
	}

	var c ServerConf
	err = conf.Unmarshal(&c)
	if err != nil { // 处理读取配置文件的错误
		log.Fatalf("Unmarshal mapstructureFile Get err #%v ", err)
	}

	return &c
}
