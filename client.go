package pg

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

type Client struct {
	db     *sql.DB
	config *Config
}

func New(ctx context.Context) (*Client, error) {
	cfg, err := ConfigFromEnv()
	if err != nil {
		return nil, fmt.Errorf("load config: %w", err)
	}

	client, err := sql.Open("pgx", cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("open postgres connection: %w", err)
	}

	client.SetMaxOpenConns(int(cfg.MaxConns))
	client.SetMaxIdleConns(int(cfg.MinConns))
	client.SetConnMaxLifetime(cfg.MaxConnLifetime)
	client.SetConnMaxIdleTime(cfg.MaxConnIdleTime)

	connectTimeout := cfg.ConnectTimeout
	if connectTimeout <= 0 {
		connectTimeout = 5 * time.Second
	}
	connectCtx, cancel := context.WithTimeout(ctx, connectTimeout)
	defer cancel()

	if err := client.PingContext(connectCtx); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}

	return &Client{db: client, config: &cfg}, nil
}

func (c *Client) Port() int {
	return c.config.Port
}

func (c *Client) Close() {
	_ = c.db.Close()
}

func (c *Client) Ping(ctx context.Context) error {
	return c.db.PingContext(ctx)
}

func (c *Client) Exec(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return c.db.ExecContext(ctx, query, args...)
}

func (c *Client) Query(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return c.db.QueryContext(ctx, query, args...)
}

func (c *Client) QueryRow(ctx context.Context, query string, args ...any) *sql.Row {
	return c.db.QueryRowContext(ctx, query, args...)
}

func (c *Client) WithTx(ctx context.Context, fn func(tx *sql.Tx) error) error {
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}

	defer func() {
		_ = tx.Rollback()
	}()

	if err := fn(tx); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit tx: %w", err)
	}
	return nil
}
