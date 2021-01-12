package etcd

import (
	"context"
	"encoding/json"
	"fmt"
	"go-transfer/config"
	"log"
	"strconv"
	"time"

	"go.etcd.io/etcd/clientv3"
)

var client *clientv3.Client

// var UpdateConfChan chan []*etcd.TopicEsConf

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
	// 监听配置改动

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

// Put 更新或新建数据
func Put(k, v string) (err error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	_, err = client.Put(ctx, k, v)
	cancel()
	if err != nil {
		return
	}
	return
}

// Get 获取 key 对应的数据
func Get(key string) (resp *clientv3.GetResponse, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	resp, err = client.Get(ctx, key)
	cancel()
	if err != nil {
		fmt.Printf("get from etcd failed, err:%v\n", err)
		return
	}
	return
}

// GetOffsetKey 获取 etcd 的 offset key
func GetOffsetKey(partition int32, topic string) string {
	return fmt.Sprintf("%s_%s_%s", config.Conf.Name, strconv.FormatInt(int64(partition), 10), topic)
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
