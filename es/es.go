package es

import (
	"context"
	"fmt"
	"go-transfer/etcd"
	"log"
	"strconv"
	"sync"
	"time"

	"github.com/olivere/elastic"
)

var client *elastic.Client

// EsDataChan 消息发送通道
var EsDataChan chan *esData

type esData struct {
	index     string
	esType    string
	topic     string
	data      *map[string]interface{}
	partition int32
	offset    int64
}

// Init 初始化
func Init(esURL string, chanMaxSize int) (err error) {
	client, err = elastic.NewClient(elastic.SetURL(esURL))
	if err != nil {
		return
	}

	log.Println("ElasticSearch connect to es success")
	EsDataChan = make(chan *esData, chanMaxSize)
	return
}

// SendToChan 发送数据到通道
func SendToChan(index, estype string, topic string, data *map[string]interface{}, partition int32, offset int64) {
	d := esData{
		index:     index,
		esType:    estype,
		topic:     topic,
		data:      data,
		partition: partition,
		offset:    offset,
	}
	EsDataChan <- &d
}

// Run 循环发送数据到 ES
func Run() {
	var wg sync.WaitGroup
	// FIXME 多个协程发送消息，从 kafka 的同一个分区同时拉取到多条数据，会导致 offset 不准确
	go func() {
		for {
			select {
			case msg := <-EsDataChan:
				// 发送消息
				sendData(msg)
			default:
				time.Sleep(time.Millisecond * 5)
			}

		}
	}()

	wg.Add(1)

	wg.Wait()

}

func sendData(msg *esData) {
	log.Printf(" kafka 发送消息 %v\n", msg.data)

	put1, err := client.Index().
		Index(msg.index).
		Type(msg.esType).
		BodyJson(msg.data).
		Do(context.Background())

	if err != nil {
		log.Printf("kafka 发送消息失败 : %v", err)
		return
	}
	log.Printf(" kafka 发送消息成功 %v  offset %d\n", msg.data, msg.offset)
	// 保存 offset 到 etcd 里
	key := etcd.GetOffsetKey(msg.partition, msg.topic)
	etcd.Put(key, strconv.FormatInt(msg.offset, 10))
	fmt.Printf("Indexed %s to index %s, type %s\n", put1.Id, put1.Index, put1.Type)
	return
}
