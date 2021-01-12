package es

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/olivere/elastic"
)

var client *elastic.Client

// EsDataChan 消息发送通道
var EsDataChan chan *EsData

type EsData struct {
	Index string
	Type  string
	Data  *map[string]interface{}
}

// Init 初始化
func Init(esURL string, chanMaxSize int) (err error) {
	client, err = elastic.NewClient(elastic.SetURL(esURL))
	if err != nil {
		return
	}

	fmt.Println("ES connect to es success")
	EsDataChan = make(chan *EsData, chanMaxSize)
	return
}

// SendToChan 发送数据到通道
func SendToChan(index, estype string, data *map[string]interface{}) {
	d := EsData{
		Index: index,
		Type:  estype,
		Data:  data,
	}
	EsDataChan <- &d
}

// Run 循环发送数据到 ES
func Run(senderNums int) {
	var wg sync.WaitGroup
	// 多个协程发送消息
	for i := 0; i < senderNums; i++ {
		go func() {
			for {
				select {
				case esData := <-EsDataChan:
					// 发送消息
					go sendData(esData.Index, esData.Type, esData.Data)
				default:
					time.Sleep(time.Millisecond * 5)
				}

			}
		}()

	}
	wg.Add(1)

	wg.Wait()

}

func sendData(index, estype string, data interface{}) {
	log.Printf(" kafka 发送消息 \n")

	put1, err := client.Index().
		Index(index).
		Type(estype).
		BodyJson(data).
		Do(context.Background())

	if err != nil {
		log.Printf("kafka 发送消息失败 : %v", err)
		return
	}
	log.Printf(" kafka 发送消息成功\n")
	fmt.Printf("Indexed %s to index %s, type %s\n", put1.Id, put1.Index, put1.Type)
	return
}
