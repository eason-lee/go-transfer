package config

// ServerConf 服务配置
type ServerConf struct {
	Name        string `yaml:"server_name"`
	KafkaConf   `yaml:"kafka"`
	EtcdConf    `yaml:"etcd"`
	EsConf `yaml:"Elasticsearch"`
}

// KafkaConf ...
type KafkaConf struct {
	Address     []string `yaml:"address,flow"`
}

// EtcdConf ...
type EtcdConf struct {
	Address []string `yaml:"address,flow"`
}


// EsConf ...
type EsConf struct {
	URL string `yaml:"url"`
	EtcdKey string `yaml:"etcd_key"`
	ChanMaxSize int      `yaml:"chan_max_size"`
	SerderNums int      `yaml:"sender_nums"`
}
