package pg

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	URL             string
	MinConns        int32
	MaxConns        int32
	MaxConnLifetime time.Duration
	MaxConnIdleTime time.Duration
	ConnectTimeout  time.Duration
	Host            string
	Port            int
	DBName          string
	User            string
	Password        string
	AsyncWorkers    int
	AsyncQueueSize  int
}

func DefaultConfig() Config {
	return Config{
		MinConns:        2,
		MaxConns:        32,
		MaxConnLifetime: 30 * time.Minute,
		MaxConnIdleTime: 5 * time.Minute,
		ConnectTimeout:  5 * time.Second,
	}
}

func ConfigFromEnv() (Config, error) {
	cfg := DefaultConfig()
	cfg.Host = getEnv("POSTGRES_HOST", "localhost")
	cfg.Port = getEnvInt("POSTGRES_PORT", 5432)
	cfg.DBName = getEnv("POSTGRES_DB_NAME", "fast")
	cfg.User = getEnv("POSTGRES_USER", "fast")
	cfg.Password = getEnv("POSTGRES_PASSWORD", "fastpass")
	cfg.URL = fmt.Sprintf("postgresql://%s:%s@%s:%d/%s?sslmode=disable", cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.DBName)
	cfg.MinConns = int32(getEnvInt("POSTGRES_MIN_CONNS", int(cfg.MinConns)))
	cfg.MaxConns = int32(getEnvInt("POSTGRES_MAX_CONNS", int(cfg.MaxConns)))
	cfg.MaxConnLifetime = getEnvDuration("POSTGRES_MAX_CONN_LIFETIME", cfg.MaxConnLifetime)
	cfg.MaxConnIdleTime = getEnvDuration("POSTGRES_MAX_CONN_IDLE_TIME", cfg.MaxConnIdleTime)
	cfg.ConnectTimeout = getEnvDuration("POSTGRES_CONNECT_TIMEOUT", cfg.ConnectTimeout)
	cfg.AsyncWorkers = getEnvInt("POSTGRES_ASYNC_WORKERS", 8)
	cfg.AsyncQueueSize = getEnvInt("POSTGRES_ASYNC_QUEUE", 128)

	if cfg.URL == "" {
		return Config{}, fmt.Errorf("Missing Postgres URL configuration")
	}
	return cfg, nil
}

func getEnv(key, fallback string) string {
	value, ok := os.LookupEnv(key)
	if !ok || value == "" {
		return fallback
	}
	return value
}

func getEnvInt(key string, fallback int) int {
	value, ok := os.LookupEnv(key)
	if !ok || value == "" {
		return fallback
	}

	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func getEnvDuration(key string, fallback time.Duration) time.Duration {
	value, ok := os.LookupEnv(key)
	if !ok || value == "" {
		return fallback
	}

	parsed, err := time.ParseDuration(value)
	if err != nil {
		return fallback
	}
	return parsed
}
