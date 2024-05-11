package ch

import (
	"context"
	"errors"
	"fmt"
	"limits-provider/migrations"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/pressly/goose/v3"
)

func UpMigrations(ctx context.Context, dsn string) error {
	opts, err := clickhouse.ParseDSN(dsn)
	if err != nil {
		return fmt.Errorf("parse ch dsn: %w", err)
	}

	db := clickhouse.OpenDB(opts)

	goose.SetBaseFS(migrations.FS)
	goose.SetLogger(goose.NopLogger())

	if err = goose.SetDialect("clickhouse"); err != nil {
		return fmt.Errorf("set goose dialect: %w", err)
	}

	err = goose.UpContext(ctx, db, "clickhouse")
	if err != nil && !errors.Is(err, goose.ErrNoNextVersion) {
		return fmt.Errorf("up migrations: %w", err)
	}

	return nil
}
