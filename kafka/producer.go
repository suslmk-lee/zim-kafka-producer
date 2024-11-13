package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
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
		// 모든 복제본에 메시지가 기록될 때까지 대기하도록 설정 (-1)
		RequiredAcks: -1,
		Async:        false, // 동기 전송 사용
	})

	return writer, nil
}

// Kafka로 IoT 데이터를 전송하는 함수
func SendDataToKafka(ctx context.Context, writer *kafka.Writer, data db.IoTData) error {
	// 고유 MessageID 생성 및 할당
	data.MessageID = uuid.NewString()

	// 타임스탬프를 UTC 형식으로 변환
	data.Timestamp = data.Timestamp.UTC()

	message, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("Error marshalling data to JSON: %v", err)
	}

	// 전송 재시도 로직
	for retries := 0; retries < 3; retries++ {
		err = writer.WriteMessages(ctx, kafka.Message{
			Key:   []byte(data.MessageID), // 고유 MessageID를 Key로 사용하여 중복 방지
			Value: message,
		})
		if err == nil {
			return nil
		}
		// 전송 실패 시 대기 후 재시도
		time.Sleep(500 * time.Millisecond)
	}

	return fmt.Errorf("Failed to send data to Kafka after retries: %v", err)
}
