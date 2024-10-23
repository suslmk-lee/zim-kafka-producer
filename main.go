package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/segmentio/kafka-go"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
	"zim-kafka-producer/common"
)

const (
	kafkaTopic  = "iot-data-topic" // Kafka 토픽
	kafkaBroker = "localhost:9092" // Kafka 브로커 주소
)

type IoTData struct {
	Humidity       float64 `json:"humidity"`
	Temperature    float64 `json:"temperature"`
	LightQuantity  float64 `json:"light_quantity"`
	BatteryVoltage float64 `json:"battery_voltage"`
	SolarVoltage   float64 `json:"solar_voltage"`
	LoadAmpere     float64 `json:"load_ampere"`
	Timestamp      string  `json:"timestamp"`
}

func main() {
	// Graceful shutdown 위한 context 및 signal 채널 설정
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// PostgreSQL 연결 설정 (연결 풀 사용)
	dbpool, err := connectToDB()
	if err != nil {
		log.Fatalf("Unable to connect to database: %v\n", err)
	}
	defer dbpool.Close()

	// Kafka Producer 설정
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{kafkaBroker},
		Topic:    kafkaTopic,
		Balancer: &kafka.LeastBytes{},
	})
	defer writer.Close()

	// 데이터베이스에서 주기적으로 데이터 읽어오기
	go func() {
		for {
			select {
			case <-ctx.Done():
				log.Println("Shutting down producer service...")
				return
			default:
				// PostgreSQL에서 데이터 읽기
				data, err := readIoTData(dbpool)
				if err != nil {
					log.Printf("Error reading data from DB: %v\n", err)
					time.Sleep(5 * time.Second)
					continue
				}

				// Kafka로 데이터 전송
				err = sendDataToKafka(ctx, writer, data)
				if err != nil {
					log.Printf("Error sending data to Kafka: %v\n", err)
				} else {
					log.Printf("Data sent to Kafka at %v\n", data.Timestamp)
				}

				// 데이터 전송 간격 (1초)
				time.Sleep(1 * time.Second)
			}
		}
	}()

	// Graceful shutdown 대기
	<-sigChan
	cancel()
	time.Sleep(2 * time.Second) // 잠시 대기 후 종료
}

// PostgreSQL 연결 풀 생성
func connectToDB() (*pgxpool.Pool, error) {
	dbURL := getDataSource()

	config, err := pgxpool.ParseConfig(dbURL)
	if err != nil {
		return nil, fmt.Errorf("Unable to parse database config: %v", err)
	}

	dbpool, err := pgxpool.ConnectConfig(context.Background(), config)
	if err != nil {
		return nil, fmt.Errorf("Unable to create connection pool: %v", err)
	}

	return dbpool, nil
}

// PostgreSQL에서 IoT 데이터를 읽어오는 함수
func readIoTData(pool *pgxpool.Pool) (IoTData, error) {
	var data IoTData
	err := pool.QueryRow(context.Background(), `
		SELECT humidity, temperature, light_quantity, battery_voltage, solar_voltage, load_ampere, timestamp
		FROM iot_data
		ORDER BY timestamp DESC LIMIT 1`).Scan(
		&data.Humidity, &data.Temperature, &data.LightQuantity, &data.BatteryVoltage, &data.SolarVoltage, &data.LoadAmpere, &data.Timestamp)

	if err != nil {
		return IoTData{}, fmt.Errorf("Error querying data: %v", err)
	}

	return data, nil
}

// Kafka로 IoT 데이터를 전송하는 함수
func sendDataToKafka(ctx context.Context, writer *kafka.Writer, data IoTData) error {
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

// 데이터베이스 연결 정보 구성
func getDataSource() string {
	var databaseHost, databaseName, databaseUser, databasePassword, databasePort string

	// 환경 변수에서 프로파일 가져오기 (없으면 기본값 "local")
	profile := "local"
	if len(os.Getenv("PROFILE")) > 0 {
		profile = os.Getenv("PROFILE")
	}

	// prod 프로파일일 경우 환경 변수에서 DB 정보 가져오기
	switch profile {
	case "prod":
		databaseHost = getEnv("DATABASE_HOST", "localhost")
		databaseName = getEnv("DATABASE_NAME", "default_prod_db")
		databaseUser = getEnv("DATABASE_USER", "default_prod_user")
		databasePassword = getEnv("DATABASE_PASSWORD", "default_prod_password")
		databasePort = getEnv("DATABASE_PORT", "5432")
	default:
		// local 프로파일일 경우 common 패키지에서 설정 정보 가져오기
		log.Println("Using local profile = ", profile)
		databaseHost = common.ConfInfo["database.host"]
		databaseName = common.ConfInfo["database.name"]
		databaseUser = common.ConfInfo["database.user"]
		databasePassword = common.ConfInfo["database.password"]
		databasePort = common.ConfInfo["database.port"]
		log.Println(databaseUser)
	}

	// PostgreSQL 데이터 소스 문자열 구성
	dataSource := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable", databaseUser, databasePassword, databaseHost, databasePort, databaseName)
	return dataSource
}

// getEnv 함수 - 환경 변수 가져오기
func getEnv(key, defaultValue string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultValue
}
