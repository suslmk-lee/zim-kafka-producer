package db

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
	"zim-kafka-producer/common"
)

// IoTData 구조체 정의
type IoTData struct {
	Humidity       float64   `json:"humidity"`
	Temperature    float64   `json:"temperature"`
	LightQuantity  float64   `json:"light_quantity"`
	BatteryVoltage float64   `json:"battery_voltage"`
	SolarVoltage   float64   `json:"solar_voltage"`
	LoadAmpere     float64   `json:"load_ampere"`
	Timestamp      time.Time `json:"timestamp"`
}

// PostgreSQL 연결 풀 생성
func ConnectToDB() (*pgxpool.Pool, error) {
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

// PostgreSQL에서 여러 레코드를 읽어오는 함수
func ReadIoTDataBatch(pool *pgxpool.Pool) ([]IoTData, error) {
	rows, err := pool.Query(context.Background(), `
		SELECT humidity, temperature, light_quantity, battery_voltage, solar_voltage, load_ampere, timestamp
		FROM iot_data
		ORDER BY timestamp DESC LIMIT 10`)
	if err != nil {
		return nil, fmt.Errorf("Error querying data: %v", err)
	}
	defer rows.Close()

	var dataBatch []IoTData
	for rows.Next() {
		var data IoTData
		err := rows.Scan(&data.Humidity, &data.Temperature, &data.LightQuantity, &data.BatteryVoltage, &data.SolarVoltage, &data.LoadAmpere, &data.Timestamp)
		if err != nil {
			return nil, fmt.Errorf("Error scanning row: %v", err)
		}
		dataBatch = append(dataBatch, data)
	}

	return dataBatch, nil
}

// 데이터베이스 연결 정보 구성
func getDataSource() string {
	return fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable",
		common.ConfInfo["database.user"],
		common.ConfInfo["database.password"],
		common.ConfInfo["database.host"],
		common.ConfInfo["database.port"],
		common.ConfInfo["database.name"])
}
