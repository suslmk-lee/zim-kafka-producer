package kafka

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/segmentio/kafka-go"
	"zim-kafka-producer/config"
	"zim-kafka-producer/db"
)

// Kafka Producer 생성 함수
func NewProducer() (*kafka.Writer, error) {
	topic := config.GetConfig("KAFKA_TOPIC", "iot-data-topic")
	broker := config.GetConfig("KAFKA_BROKER", "localhost:9092")

	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{broker},
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	})

	return writer, nil
}

// Kafka로 IoT 데이터를 전송하는 함수
func SendDataToKafka(ctx context.Context, writer *kafka.Writer, data db.IoTData) error {
	data.Timestamp = data.Timestamp.UTC()

	message, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("Error marshalling data to JSON: %v", err)
	}

	err = writer.WriteMessages(ctx, kafka.Message{
		Value: message,
	})
	if err != nil {
		return fmt.Errorf("Error writing message to Kafka: %v", err)
	}

	return nil
}
