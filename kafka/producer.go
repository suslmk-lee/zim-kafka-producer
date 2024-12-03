package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"zim-kafka-producer/config"
	"zim-kafka-producer/db"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
)

// Kafka Producer 생성 함수
func NewProducer() (*kafka.Writer, error) {
	// Kafka 토픽 및 브로커 URL 설정
	topic := config.GetConfig("KAFKA_TOPIC", "iot-data-topic")
	brokerHost := config.GetConfig("KAFKA_HOST", "localhost")
	brokerPort := config.GetConfig("KAFKA_PORT", "9092")
	brokerURL := fmt.Sprintf("%s:%s", brokerHost, brokerPort)

	// Kafka Writer 설정
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:      []string{brokerURL},
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		BatchSize:    10,                     // Batch 처리 크기 설정
		BatchTimeout: 500 * time.Millisecond, // Batch 타임아웃 설정
	})

	return writer, nil
}

// Kafka로 IoT 데이터를 전송하는 함수
func SendDataToKafka(ctx context.Context, writer *kafka.Writer, data db.IoTData) error {
	// 고유 MessageID 생성 및 할당
	data.MessageID = uuid.NewString()

	// 타임스탬프를 UTC 형식으로 변환
	data.Timestamp = data.Timestamp.UTC()

	// 메시지 직렬화
	message, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("Error marshalling data to JSON: %v", err)
	}

	// Kafka 메시지 전송 (재시도 포함)
	const maxRetries = 3
	var retryInterval = 500 * time.Millisecond
	for attempt := 1; attempt <= maxRetries; attempt++ {
		err = writer.WriteMessages(ctx, kafka.Message{
			Key:   []byte(data.MessageID), // 메시지 고유 ID를 Key로 사용
			Value: message,
		})
		if err == nil {
			// 전송 성공 시 로그 출력 및 종료
			fmt.Printf("Message sent to Kafka (MessageID: %s, Timestamp: %s)\n", data.MessageID, data.Timestamp)
			return nil
		}

		// 전송 실패 시 재시도
		fmt.Printf("Retry %d/%d: Failed to send message (MessageID: %s, Error: %v)\n", attempt, maxRetries, data.MessageID, err)
		time.Sleep(retryInterval)
		retryInterval *= 2 // 지수 백오프 적용
	}

	// 최대 재시도 후 실패
	return fmt.Errorf("Failed to send message to Kafka after %d attempts: %v", maxRetries, err)
}
