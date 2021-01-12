package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"go-transfer/es"
	"go-transfer/etcd"
	"log"

	"github.com/Shopify/sarama"
)

// ConsumerTask 消费任务
type ConsumerTask struct {
	Topic     string
	Consumers []*sarama.PartitionConsumer
	Ctx       context.Context
	Cancel    context.CancelFunc
}

// ConsumerTaskManager 任务管理
type ConsumerTaskManager struct {
	TaskMap map[string]*ConsumerTask
	// 配置修改的通道
	UpdateConfChan chan []*etcd.TopicEsConf
}

// TaskManager 任务管理器
var TaskManager = ConsumerTaskManager{
	TaskMap:        make(map[string]*ConsumerTask, 64),
	UpdateConfChan: make(chan []*etcd.TopicEsConf),
}

// NewConsumerTask 创建一个消费者
func (m *ConsumerTaskManager) NewConsumerTask(address []string, topic, esIndex, esType string) {

	// 创建消费任务
	ctx, cancel := context.WithCancel(context.Background())
	partitionCons := createConsumer(ctx, address, topic, esIndex, esType)
	task := ConsumerTask{
		Topic:     topic,
		Consumers: partitionCons,
		Ctx:       ctx,
		Cancel:    cancel,
	}
	TaskManager.TaskMap[topic] = &task
}

func createConsumer(ctx context.Context, address []string, topic, esIndex, esType string) (partitionCons []*sarama.PartitionConsumer) {

	// 建立消费者连接 kafka
	consumer, err := sarama.NewConsumer(address, nil)
	if err != nil {
		fmt.Printf("fail to start consumer, err:%v\n", err)
		return
	}
	partitionList, err := consumer.Partitions(topic) // 根据topic取到所有的分区
	if err != nil {
		fmt.Printf("fail to get list of partition:err%v\n", err)
		return
	}

	partitionCons = make([]*sarama.PartitionConsumer, 64)
	for partition := range partitionList {
		// 针对每个分区创建一个对应的分区消费者
		pc, err := consumer.ConsumePartition(topic,
			int32(partition),
			sarama.OffsetNewest)
		if err != nil {
			fmt.Printf("failed to start consumer for partition %d,err:%v\n", partition, err)
			return
		}
		// 把消费者加入到任务列表里
		partitionCons = append(partitionCons, &pc)

		fmt.Printf("分区 消费者%v  \n", pc)

		// 异步从每个分区消费信息
		go func(sarama.PartitionConsumer) {
			for {
				select {
				case msg := <-pc.Messages():
					// 发送数据到 EsDataChan
					data := make(map[string]interface{})
					err := json.Unmarshal([]byte(msg.Value), &data)
					if err != nil {
						log.Printf("json 解析失败 : %v\n", err)
					}
					fmt.Println("Es data", data)
					es.SendToChan(esIndex, esType, &data)
				case <-ctx.Done(): // context 收到结束命令
					pc.AsyncClose()
					log.Printf("ConsumerTask 任务结束 \n")
					return
				default:
				}
			}

		}(pc)

	}
	return

}

// ListenUpdateConf 监听配置改动
func (m *ConsumerTaskManager) ListenUpdateConf(address []string) {
	log.Println("启动监听配置改动")
	for {
		select {
		case up := <-m.UpdateConfChan:
			for _, confg := range up {
				// 已有的配置不做改动
				log.Printf("配置更新 %v\n", confg)
				_, ok := TaskManager.TaskMap[confg.Topic]
				if ok {
					continue
				} else {
					// 创建新的 TailTask
					TaskManager.NewConsumerTask(
						address,
						confg.Topic,
						confg.Index,
						confg.Type,
					)
				}

			}
			// 删除修改和删除的 TailTask
			for _, existTask := range TaskManager.TaskMap {
				isDelete := true
				for _, newConf := range up {
					if newConf.Topic == existTask.Topic {
						isDelete = false
						continue
					}
				}
				if isDelete {
					existTask.Cancel()
					delete(TaskManager.TaskMap, existTask.Topic)
				}
			}
			log.Printf("配置更新：%v", up)
		default:
		}
	}
}
