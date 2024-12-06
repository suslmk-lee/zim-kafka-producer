package db

import (
	"context"
	"fmt"
	"zim-kafka-producer/config"

	"github.com/jackc/pgx/v4/pgxpool"
)

// IoTData 구조체 정의
type IoTData struct {
	MessageID      string  `json:"message_id"`
	Device         string  `json:"Device"`
	Timestamp      int64   `json:"Timestamp"`
	ProVer         int     `json:"ProVer"`
	MinorVer       int     `json:"MinorVer"`
	SN             int64   `json:"SN"`
	Model          string  `json:"model"`
	Tyield         float64 `json:"Tyield"`
	Dyield         float64 `json:"Dyield"`
	PF             float64 `json:"PF"`
	Pmax           float64 `json:"Pmax"`
	Pac            float64 `json:"Pac"`
	Sac            float64 `json:"Sac"`
	Uab            float64 `json:"Uab"`
	Ubc            float64 `json:"Ubc"`
	Uca            float64 `json:"Uca"`
	Ia             float64 `json:"Ia"`
	Ib             float64 `json:"Ib"`
	Ic             float64 `json:"Ic"`
	Freq           float64 `json:"Freq"`
	Tmod           float64 `json:"Tmod"`
	Tamb           float64 `json:"Tamb"`
	Mode           string  `json:"Mode"`
	Qac            int     `json:"Qac"`
	BusCapacitance float64 `json:"BusCapacitance"`
	AcCapacitance  float64 `json:"AcCapacitance"`
	Pdc            float64 `json:"Pdc"`
	PmaxLim        float64 `json:"PmaxLim"`
	SmaxLim        float64 `json:"SmaxLim"`
}

// PostgreSQL 연결 풀 생성
func ConnectToDB() (*pgxpool.Pool, error) {
	dbURL := config.GetDataSource() // 환경에 따라 설정 값을 가져옴
	fmt.Println("dbURL: ", dbURL)

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
func ReadUnprocessedData(pool *pgxpool.Pool) ([]IoTData, error) {
	rows, err := pool.Query(context.Background(), `
		SELECT 
			device, timestamp, pro_ver, minor_ver, sn, model, 
			tyield, dyield, pf, pmax, pac, sac, uab, ubc, uca, 
			ia, ib, ic, freq, tmod, tamb, mode, qac, bus_capacitance, 
			ac_capacitance, pdc, pmax_lim, smax_lim
		FROM iot_data
		WHERE is_sent = FALSE
		ORDER BY timestamp ASC
		LIMIT 10`)
	if err != nil {
		return nil, fmt.Errorf("Error querying data: %v", err)
	}
	defer rows.Close()

	var dataBatch []IoTData
	for rows.Next() {
		var data IoTData
		var timestamp int64
		err := rows.Scan(
			&data.Device, &timestamp, &data.ProVer, &data.MinorVer, &data.SN, &data.Model,
			&data.Tyield, &data.Dyield, &data.PF, &data.Pmax, &data.Pac, &data.Sac,
			&data.Uab, &data.Ubc, &data.Uca, &data.Ia, &data.Ib, &data.Ic,
			&data.Freq, &data.Tmod, &data.Tamb, &data.Mode, &data.Qac,
			&data.BusCapacitance, &data.AcCapacitance, &data.Pdc, &data.PmaxLim, &data.SmaxLim,
		)
		if err != nil {
			return nil, fmt.Errorf("Error scanning row: %v", err)
		}
		data.Timestamp = timestamp
		dataBatch = append(dataBatch, data)
	}

	return dataBatch, nil
}

func MarkDataAsSent(pool *pgxpool.Pool, data IoTData) error {
	_, err := pool.Exec(context.Background(), `
		UPDATE iot_data
		SET is_sent = TRUE
		WHERE timestamp = $1`, data.Timestamp)
	return err
}
