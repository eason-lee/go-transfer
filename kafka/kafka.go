package kafka

import (
	"context"
	"go-transfer/config"
	"go-transfer/service"
	"io"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	defaultCommitInterval = time.Second
	firstOffset           = "first"
	lastOffset            = "last"
)

type (
	ConsumeHandler interface {
		Consume(key, value string) error
	}
	MessageQueue interface {
		Start()
		Stop()
	}

	kafkaQueue struct {
		c                *config.KqConf
		consumer         *kafka.Reader
		handler          ConsumeHandler
		channel          chan kafka.Message
		producerRoutines *service.RoutineGroup
		consumerRoutines *service.RoutineGroup
	}

	kafkaQueues struct {
		queues []MessageQueue
		group  *service.ServiceGroup
	}
)

func (q *kafkaQueue) Start() {
	q.startConsumers()
	q.startProducers()

	q.producerRoutines.Wait()
	close(q.channel)
	q.consumerRoutines.Wait()
}

func (q *kafkaQueue) Stop() {
	q.consumer.Close()
}

func (q kafkaQueues) Start() {
	for _, each := range q.queues {
		q.group.Add(each)
	}
	q.group.Start()
}

func (q kafkaQueues) Stop() {
	q.group.Stop()
}

func (q *kafkaQueue) consumeOne(key, val string) error {
	err := q.handler.Consume(key, val)
	return err
}

// NewQueue ...
func NewQueue(c *config.KqConf, handler ConsumeHandler) MessageQueue {

	if c.Conns < 1 {
		c.Conns = 1
	}
	q := kafkaQueues{
		group: service.NewServiceGroup(),
	}

	for i := 0; i < c.Conns; i++ {
		q.queues = append(q.queues, newKafkaQueue(c, handler))
	}

	return q
}

func newKafkaQueue(c *config.KqConf, handler ConsumeHandler) MessageQueue {
	var offset int64
	if c.Offset == firstOffset {
		offset = kafka.FirstOffset
	} else {
		offset = kafka.LastOffset
	}

	consumer := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        c.Brokers,
		GroupID:        c.Group,
		Topic:          c.Topic,
		StartOffset:    offset,
		MinBytes:       c.MinBytes,
		MaxBytes:       c.MaxBytes,
		CommitInterval: defaultCommitInterval,
	})

	return &kafkaQueue{
		c:                c,
		consumer:         consumer,
		handler:          handler,
		channel:          make(chan kafka.Message),
		producerRoutines: service.NewRoutineGroup(),
		consumerRoutines: service.NewRoutineGroup(),
	}
}

func (q *kafkaQueue) startConsumers() {
	for i := 0; i < q.c.Processors; i++ {
		q.consumerRoutines.Run(func() {
			for msg := range q.channel {
				if err := q.consumeOne(string(msg.Key), string(msg.Value)); err != nil {
					log.Printf("Error on consuming: %s, error: %v", string(msg.Value), err)
				}
				q.consumer.CommitMessages(context.Background(), msg)
			}
		})
	}
}

func (q *kafkaQueue) startProducers() {
	for i := 0; i < q.c.Consumers; i++ {
		q.producerRoutines.Run(func() {
			for {
				msg, err := q.consumer.FetchMessage(context.Background())
				if err == io.EOF || err == io.ErrClosedPipe {
					return
				}
				if err != nil {
					log.Printf("Error on reading mesage, %q", err.Error())
					continue
				}
				q.channel <- msg
			}
		})
	}
}
