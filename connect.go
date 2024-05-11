package ch

import (
	"context"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"sync"
	"time"
)

const hashesChanCap = 10000

type Repository struct {
	conn        driver.Conn
	batchesPool *sync.Pool
	dataChan    chan any
	logger      Logger
	enabled     bool
}

type Logger interface {
	Errorf(format string, args ...interface{})
	Infof(format string, args ...interface{})
	Debugf(format string, args ...interface{})
	Debugw(msg string, keysAndValues ...interface{})
}

func NewRepository(log Logger, enabled bool) *Repository {
	r := new(Repository)
	r.dataChan = make(chan entity.LimitHashEntity, hashesChanCap)
	r.batchesPool = initBatchPool()
	r.logger = log
	r.enabled = enabled

	return r
}
func (repo *Repository) Connect(ctx context.Context, cfg app.Clickhouse) error {
	const (
		pingInterval = time.Second
		pingCount    = 4
	)

	repo.logger.Infof("saving hashes is %t", repo.enabled)
	repo.enabled = cfg.Enabled

	opts, err := clickhouse.ParseDSN(cfg.DSN)
	if err != nil {
		return fmt.Errorf("parsing ch dsn: %w", err)
	}

	conn, err := clickhouse.Open(opts)
	if err != nil {
		return fmt.Errorf("connect ch:%w", err)
	}

	for i := 0; i < pingCount; i++ {
		if err = conn.Ping(ctx); err == nil {
			repo.logger.Debug("connected to ch")

			break
		}

		time.Sleep(pingInterval)
	}

	if err != nil {
		return fmt.Errorf("ping ch: %w", err)
	}

	repo.conn = conn

	return nil
}

func (repo *Repository) ProcessHashes(ctx context.Context, saveInterval time.Duration) {
	repo.logger.Debugw("process hashes started", "interval", saveInterval.Seconds())
	defer repo.logger.Debugw("process hashes finished")
	repo.processLimitHashes(ctx, saveInterval, maxBatchCap)
}
