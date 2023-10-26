package main

import (
	"context"
	"flag"
	"fmt"
	"image-consumer/consumer"
	_ "image/jpeg"
	_ "image/png"

	"github.com/IBM/sarama"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

func loadConfig() {
	configFilePath := flag.String("config", "conf/", "Path to config file")
	flag.Parse()
	fmt.Println("Config file path:", *configFilePath)

	viper.SetConfigName("app")
	viper.AddConfigPath(*configFilePath)
	viper.SetConfigType("yaml")
	if err := viper.ReadInConfig(); err != nil {
		panic(fmt.Errorf("Fatal error config file: %s", err))
	}
	logrus.Info("Config file loaded successfully")
}

func init() {
	loadConfig()
}

func main() {
	partitionConsumer, err := consumer.InitConsumer([]string{viper.GetString("kafka.bootstrapServer")}, []string{viper.GetString("kafka.topic")})
	if err != nil {
		logrus.Fatalf("error initializing consumer: %v", err)
		return
	}

	consumer.ConsumeMessages(context.Background(), partitionConsumer, func(msg *sarama.ConsumerMessage) {
		logrus.Infof("Received message: %s", string(msg.Value))

		fmt.Println("Message received", string(msg.Value))
	})

}
