package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"image-consumer/consumer"
	"image-consumer/imageutils"
	"image-consumer/utils"
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
	host := viper.GetString("host")

	partitionConsumer, err := consumer.InitConsumer([]string{viper.GetString("kafka.bootstrapServer")}, []string{viper.GetString("kafka.topic")})
	if err != nil {
		logrus.Fatalf("error initializing consumer: %v", err)
		return
	}

	consumer.ConsumeMessages(context.Background(), partitionConsumer, func(msg *sarama.ConsumerMessage) {
		logrus.Infof("Received message: %s", string(msg.Value))

		// Get All Image URLs
		url := fmt.Sprintf("%s%s%s", host, "/products/images/", string(msg.Value))
		resp, err := utils.MakeRequest(url, "GET", nil)
		if err != nil {
			logrus.Errorf("error making request: %v", err)
			return
		}

		if resp.StatusCode != 200 {
			logrus.Errorf("error getting product images: %v", err)
			return
		}

		// Extract Image URLs
		var productImages []string
		err = json.NewDecoder(resp.Body).Decode(&productImages)
		if err != nil {
			logrus.Errorf("error decoding response body: %v", err)
			return
		}
		logrus.Infof("Found %d images for product %s", len(productImages), string(msg.Value))

		var compressedImages []string = make([]string, len(productImages))
		// Compress and Store Images
		for i, productImage := range productImages {
			logrus.Infof("Compressing image: %s", productImage)
			compressedImagePath, err := imageutils.CompressImage(productImage)
			if err != nil {
				logrus.Errorf("error compressing image: %v", err)
				return
			}
			compressedImages[i] = compressedImagePath
		}
		logrus.Info("Compressed Images: ", compressedImages)
		// Update compressed_product_images table

	})

}
