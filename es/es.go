package es

import (
	"context"
	"go-transfer/config"
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

// 保存 offset 到 etcd
var offsetToEtcdChan chan *esData

type esData struct {
	index     string
	esType    string
	topic     string
	data      *map[string]interface{}
	partition int32
	offset    int64
}

// Init 初始化
func init() {
	var err error
	client, err = elastic.NewClient(elastic.SetURL(config.Conf.EsConf.URL))
	if err != nil {
		log.Fatalf("ElasticSearch 连接失败, err: %v\n", err)
	}

	EsDataChan = make(chan *esData, config.Conf.EsConf.ChanMaxSize)
	if config.Conf.EnabledEsOffset {
		// TODO 可使用 MarkOffset 把 offset 写入到 kafka 里，提高性能
		offsetToEtcdChan = make(chan *esData, config.Conf.EsConf.ChanMaxSize)
		// 开启更新 etcd offset
		go sendOffsetToEtcd()
	}

	log.Println("ElasticSearch 连接成功")

}

// Run 发送数据到 ES
func Run() {
	var wg sync.WaitGroup

	for i := 0; i < config.Conf.SerderNums; i++ {
		go func() {
			for {
				select {
				case msg := <-EsDataChan:
					sendData(msg)
				default:
					time.Sleep(time.Millisecond * 5)
				}

			}
		}()
	}
	wg.Add(1)

	wg.Wait()

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
	if config.Conf.EnabledEsOffset {
		offsetToEtcdChan <- &d
	}
}

func sendData(msg *esData) {
	_, err := client.Index().
		Index(msg.index).
		Type(msg.esType).
		BodyJson(msg.data).
		Do(context.Background())

	if err != nil {
		log.Printf("kafka 发送消息失败 : %v", err)
		// TODO 发送失败后重试或者错误上报
		return
	}
	log.Printf(" kafka 发送消息成功 %v  offset %d\n", msg.data, msg.offset)

	return
}

func sendOffsetToEtcd() {
	for {
		select {
		case msg := <-offsetToEtcdChan:
			// 保存 offset 到 etcd 里
			// 这里只有一个协程处理，会导致更新offset的速度，没有发送到es的速度快，程序意外退出时还有offset滞后问题
			key := etcd.GetOffsetKey(msg.partition, msg.topic)
			etcd.Put(key, strconv.FormatInt(msg.offset, 10))
			log.Printf(" kafka 记录 offset 成功 %v  offset %d\n", msg.data, msg.offset)
		}

	}

}
