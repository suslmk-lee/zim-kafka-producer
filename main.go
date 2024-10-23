package main

import (
	"context"
	"github.com/sirupsen/logrus"
	"os"
	"os/signal"
	"syscall"
	"time"
	"zim-kafka-producer/db"
	"zim-kafka-producer/kafka"
)

var logger = logrus.New()

func init() {
	logger.SetFormatter(&logrus.JSONFormatter{})
	logger.SetOutput(os.Stdout)
	logger.SetLevel(logrus.InfoLevel)
}

func main() {
	// Graceful shutdown 위한 context 및 signal 채널 설정
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// PostgreSQL 연결
	dbPool, err := db.ConnectToDB()
	if err != nil {
		logger.Fatalf("Unable to connect to database: %v\n", err)
	}
	defer dbPool.Close()

	// Kafka Producer 설정
	kafkaWriter, err := kafka.NewProducer()
	if err != nil {
		logger.Fatalf("Unable to create Kafka producer: %v\n", err)
	}
	defer kafkaWriter.Close()

	// 데이터베이스에서 주기적으로 데이터 읽어오기 및 Kafka 전송
	go func() {
		for {
			select {
			case <-ctx.Done():
				logger.Info("Shutting down producer service...")
				return
			default:
				dataBatch, err := db.ReadIoTDataBatch(dbPool)
				if err != nil {
					logger.Errorf("Error reading data from DB: %v\n", err)
					time.Sleep(5 * time.Second)
					continue
				}

				// Kafka로 데이터 비동기 전송
				for _, data := range dataBatch {
					go func(d db.IoTData) {
						err := kafka.SendDataToKafka(ctx, kafkaWriter, d)
						if err != nil {
							logger.Errorf("Error sending data to Kafka: %v\n", err)
						} else {
							logger.Infof("Data sent to Kafka at %v\n", d.Timestamp)
						}
					}(data)
				}

				time.Sleep(1 * time.Second)
			}
		}
	}()

	// Graceful shutdown 대기
	<-sigChan
	cancel()
	time.Sleep(2 * time.Second)
}
