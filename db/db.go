package db

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v4/pgxpool"
	"time"
	"zim-kafka-producer/config"
)

// IoTData 구조체 정의
type IoTData struct {
	MessageID string    `json:"message_id"`
	Device    string    `json:"Device"`
	Timestamp time.Time `json:"Timestamp"`
	ProVer    int       `json:"ProVer"`
	MinorVer  int       `json:"MinorVer"`
	SN        int64     `json:"SN"`
	Model     string    `json:"model"`
	Status    Status    `json:"Status"`
}

// Status 구조체 정의
type Status struct {
	Tyield         float64 `json:"Tyield"`
	Dyield         float64 `json:"Dyield"`
	PF             float64 `json:"PF"`
	Pmax           int     `json:"Pmax"`
	Pac            int     `json:"Pac"`
	Sac            int     `json:"Sac"`
	Uab            int     `json:"Uab"`
	Ubc            int     `json:"Ubc"`
	Uca            int     `json:"Uca"`
	Ia             int     `json:"Ia"`
	Ib             int     `json:"Ib"`
	Ic             int     `json:"Ic"`
	Freq           int     `json:"Freq"`
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
		var status Status
		err := rows.Scan(
			&data.Device, &data.Timestamp, &data.ProVer, &data.MinorVer, &data.SN, &data.Model,
			&status.Tyield, &status.Dyield, &status.PF, &status.Pmax, &status.Pac, &status.Sac,
			&status.Uab, &status.Ubc, &status.Uca, &status.Ia, &status.Ib, &status.Ic,
			&status.Freq, &status.Tmod, &status.Tamb, &status.Mode, &status.Qac,
			&status.BusCapacitance, &status.AcCapacitance, &status.Pdc, &status.PmaxLim, &status.SmaxLim,
		)
		if err != nil {
			return nil, fmt.Errorf("Error scanning row: %v", err)
		}
		data.Status = status
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
