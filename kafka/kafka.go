package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"go-transfer/config"
	"go-transfer/es"
	"go-transfer/etcd"
	"log"
	"strconv"

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
	// todo 这个放到 etcd 里
	UpdateConfChan chan []*etcd.TopicEsConf
}

// TaskManager 任务管理器
var TaskManager = ConsumerTaskManager{
	TaskMap:        make(map[string]*ConsumerTask, 64),
	UpdateConfChan: make(chan []*etcd.TopicEsConf),
}
var consumer sarama.Consumer

// Init 初始化
func Init() {
	// 建立消费者连接 kafka
	var err error
	consumer, err = sarama.NewConsumer(config.Conf.KafkaConf.Address, nil)
	if err != nil {
		fmt.Printf("kafka 启动 consumer 失败, err:%v\n", err)
		return
	}
	// 获取配置
	key := fmt.Sprintf(config.Conf.EtcdKey, config.Conf.Name)
	esConfs, err := etcd.GetTopicEsConf(key)
	if err != nil {
		log.Printf("从 etcd 获取配置失败：%v", err)
	}
	log.Printf("获取 etcd 配置成功 \n")

	// 根据配置创建 消费者任务
	for _, esConf := range esConfs {
		// 初始化 kafka
		TaskManager.NewConsumerTask(
			esConf.Topic,
			esConf.Index,
			esConf.Type,
		)
		log.Println("kafka 初始化成功, topic: ", esConf.Topic)

	}

	// 监听配置修改
	go TaskManager.listenUpdateConf()

}

// NewConsumerTask 创建一个消费者
func (m *ConsumerTaskManager) NewConsumerTask(topic, esIndex, esType string) {

	// 创建消费任务
	ctx, cancel := context.WithCancel(context.Background())
	partitionCons := createConsumer(ctx, topic, esIndex, esType)
	task := ConsumerTask{
		Topic:     topic,
		Consumers: partitionCons,
		Ctx:       ctx,
		Cancel:    cancel,
	}
	TaskManager.TaskMap[topic] = &task
}

// getFileOffset 获取文件的 offset
func getFileOffset(partition int32, topic string) (offset int64) {
	key := etcd.GetOffsetKey(partition, topic)
	offsetResp, err := etcd.Get(key)
	offset = int64(0)

	if err == nil {
		for _, ev := range offsetResp.Kvs {
			if ev.Value != nil {
				offset, err = strconv.ParseInt(string(ev.Value), 10, 64)
				// offset +1 取上次消费完的下一条数据
				offset++
				break
			}

		}
	}
	return
}

func createConsumer(ctx context.Context, topic, esIndex, esType string) (partitionCons []*sarama.PartitionConsumer) {

	partitionList, err := consumer.Partitions(topic) // 根据topic取到所有的分区
	if err != nil {
		fmt.Printf("kafka 获取 partition 列表失败:err%v\n", err)
		return
	}

	partitionCons = make([]*sarama.PartitionConsumer, 64)
	for partition := range partitionList {
		// 从 etcd 里获取 offset
		offset := getFileOffset(int32(partition), topic)
		if offset == int64(0) {
			offset = sarama.OffsetNewest
		}

		// 针对每个分区创建一个对应的分区消费者
		pc, err := consumer.ConsumePartition(topic,
			int32(partition),
			offset)
		if err != nil {
			log.Printf("启动 kafka Partition 消费者失败， partition %d，err:%v\n", partition, err)
			return
		}
		log.Printf("创建 ConsumePartition 成功， offset: %d\n", offset)
		// 把消费者加入到任务列表里
		partitionCons = append(partitionCons, &pc)

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
						continue
					}
					fmt.Println("Es data", data)
					es.SendToChan(esIndex, esType, msg.Topic, &data, msg.Partition, msg.Offset)
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
func (m *ConsumerTaskManager) listenUpdateConf() {
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
