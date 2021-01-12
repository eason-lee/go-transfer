package etcd

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"go.etcd.io/etcd/clientv3"
)

var client *clientv3.Client

// TopicEsConf 消费 topic 对应的 es 配置
type TopicEsConf struct {
	Index string `json:"index"`
	Type  string `json:"type"`
	Topic string `json:"topic"`
}

// Init 初始化 etcd
func Init(address []string) (err error) {
	client, err = clientv3.New(clientv3.Config{
		Endpoints:   address,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return
	}
	return
}

// GetTopicEsConf 获取 topic 的 ES 配置
func GetTopicEsConf(key string) (configs []*TopicEsConf, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	resp, err := client.Get(ctx, key)
	cancel()
	if err != nil {
		fmt.Printf("get from etcd failed, err:%v\n", err)
		return
	}
	for _, ev := range resp.Kvs {
		err = json.Unmarshal(ev.Value, &configs)
		return
	}

	return

}

// WatchConf 监听配置改动
func WatchConf(key string, upChan chan []*TopicEsConf) {
	log.Printf("配置 watch 启动")
	rch := client.Watch(context.Background(), key) // <-chan WatchResponse
	for wresp := range rch {
		for _, ev := range wresp.Events {
			// 如果配置有修改，把数据发送到 chan 中
			var updateConf []*TopicEsConf
			log.Printf("监听到配置改动 %v\n", ev.Kv.Value)
			err := json.Unmarshal(ev.Kv.Value, &updateConf)
			if err != nil {
				log.Printf("WatchConf Unmarshal err: %v", err)
				continue
			}
			upChan <- updateConf

			log.Printf("监听到配置更新 Type: %s Key:%s Value:%s\n", ev.Type, ev.Kv.Key, ev.Kv.Value)
		}
	}
}
