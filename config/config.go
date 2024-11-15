package config

import (
	"fmt"
	"os"
	"zim-kafka-producer/common"
)

// GetConfig 함수 - 환경 설정을 가져오는 함수 (프로파일에 따라 환경 변수를 가져오거나 common을 사용)
func GetConfig(key, defaultValue string) string {
	profile := getProfile()

	switch profile {
	case "prod":
		// 운영 환경에서는 환경 변수에서 값을 가져옵니다.
		value := os.Getenv(key)
		if value == "" {
			return defaultValue
		}
		return value
	default:
		// 로컬 환경에서는 common 설정에서 값을 가져옵니다.
		return common.ConfInfo[key]
	}
}

// getProfile 함수 - 현재 프로파일을 결정하는 함수
func getProfile() string {
	profile := os.Getenv("PROFILE")
	fmt.Println("PROFILE: ", profile)
	if profile == "" {
		return "local" // 기본값은 로컬 프로파일
	}
	return profile
}

// GetDataSource 함수 - PostgreSQL 연결 문자열을 가져오는 함수
func GetDataSource() string {
	databaseUser := GetConfig("database.user", "default_user")
	databasePassword := GetConfig("database.password", "default_password")
	databaseHost := GetConfig("database.host", "localhost")
	databasePort := GetConfig("database.port", "5432")
	databaseName := GetConfig("database.name", "iot_db")

	// PostgreSQL 데이터 소스 문자열 구성
	return fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable",
		databaseUser, databasePassword, databaseHost, databasePort, databaseName)
}
