package consumer

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/IBM/sarama"
	"github.com/sirupsen/logrus"
)

func InitConsumer(brokers []string, topics []string) (sarama.PartitionConsumer, error) {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	consumer, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		return nil, fmt.Errorf("error creating consumer: %v", err)
	}

	partitionConsumer, err := consumer.ConsumePartition(topics[0], 0, sarama.OffsetNewest)
	if err != nil {
		return nil, fmt.Errorf("error creating partition consumer: %v", err)
	}

	return partitionConsumer, nil
}

// ConsumeMessages consumes messages from a Kafka topic and calls a callback function for each message
func ConsumeMessages(ctx context.Context, partitionConsumer sarama.PartitionConsumer, callback func(msg *sarama.ConsumerMessage)) {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	for {
		select {
		case msg := <-partitionConsumer.Messages():
			callback(msg)
		case err := <-partitionConsumer.Errors():
			logrus.Errorf("error consuming message: %v", err)
		case <-signals:
			return
		case <-ctx.Done():
			return
		}
	}
}
