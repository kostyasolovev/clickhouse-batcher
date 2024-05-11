//go:build local
// +build local

package ch

import (
	"context"
	"crypto/tls"
	"database/sql"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/require"
	"limits-provider/internal/entity"
	"limits-provider/pkg/app"
	"log"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"
)

const (
	// https://hub.docker.com/r/clickhouse/clickhouse-server/
	CLICKHOUSE_IMAGE   = "clickhouse/clickhouse-server"
	CLICKHOUSE_VERSION = "23.3-alpine"

	CLICKHOUSE_DB                        = "click"
	CLICKHOUSE_USER                      = "clickuser"
	CLICKHOUSE_PASSWORD                  = "password"
	CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT = "1"
	CLICKHOUSE_DSN                       = "clickhouse://clickuser:password@localhost:9001/click"
)

var (
	stdConn *sql.DB
	mctx    context.Context
)

func TestMain(m *testing.M) {
	var cancel context.CancelFunc
	mctx, cancel = context.WithCancel(context.Background())

	var purge func()
	var err error
	stdConn, purge, err = newClickHouse()
	if err != nil {
		log.Fatal(err)
	}

	if err = UpMigrations(mctx, CLICKHOUSE_DSN); err != nil {
		purge()
		log.Fatal(err)
	}

	code, done := 0, make(chan struct{})
	go func() {
		code = m.Run()
		close(done)
	}()

	var timer = time.NewTimer(time.Minute * 6)
	select {
	case <-timer.C:
		cancel()
		purge()
		log.Fatal("time is out")
	case <-done:
		cancel()
		purge()
		os.Exit(code)
	}
}

func TestRepository_processLimitHashes(t *testing.T) {
	type tcase struct {
		name             string
		count            uint64
		interval         time.Duration
		maxBatchCount    int
		dataChanInterval time.Duration
	}

	var testcases = []tcase{
		{"001",
			100_000,
			time.Microsecond * 100,
			90,
			time.Microsecond},
		{"002_only_ticks_cases",
			15_000,
			time.Microsecond * 100,
			900000,
			time.Microsecond},
		{"003_only_len_cases",
			8_000,
			time.Second * 100,
			100,
			time.Nanosecond},
		{"004_mixed",
			50,
			time.Millisecond * 2,
			10,
			time.Millisecond},
		{"005_mixed",
			189,
			time.Microsecond * 10,
			6,
			time.Microsecond},
	}

	repo := NewRepository(app.NoOpLogger(), false)

	err := repo.Connect(mctx, app.Clickhouse{DSN: CLICKHOUSE_DSN})
	require.NoError(t, err)
	defer repo.conn.Close()

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.TODO())
			defer cancel()

			go repo.processLimitHashes(ctx, tc.interval, tc.maxBatchCount)
			defer clearLimitsHashes(t, repo.conn)
			produceLimits(int(tc.count), repo.dataChan, tc.dataChanInterval)
			cancel()
			got := checkCount(t, repo.conn)
			require.Equal(t, int(tc.count), int(got))
		})
	}
}

func TestRepository_save(t *testing.T) {
	type tcase struct {
		name         string
		hashes       []entity.LimitHashEntity
		errAssertion require.ErrorAssertionFunc
	}

	var testcases = []tcase{
		{"001",
			[]entity.LimitHashEntity{
				{UserID: "test_user_001_Bchs76uZfZ", Hash: "hash_Bchs76uZfZ", Amount: 112233, Data: []byte("Bchs76uZfZ")},
				{UserID: "test_user_001_OYQUaaHwpm", Hash: "hash_OYQUaaHwpm", Amount: 102103, Data: []byte("OYQUaaHwpm")},
			},
			require.NoError},
		{"002",
			[]entity.LimitHashEntity{
				{UserID: "test_user_002_nmJZMXfS7B", Hash: "hash_nmJZMXfS7B", Amount: 2002, Data: []byte("nmJZMXfS7B")},
				{UserID: "test_user_002_MKpRPLGFjq", Hash: "hash_MKpRPLGFjq", Amount: 2003, Data: []byte("MKpRPLGFjq")},
				{UserID: "test_user_002_FcmEJWC32X", Hash: "hash_FcmEJWC32X", Amount: 2004, Data: []byte("FcmEJWC32X")},
				{UserID: "test_user_002_nWBl76MnpG", Hash: "hash_nWBl76MnpG", Amount: 2005, Data: []byte("nWBl76MnpG")},
			},
			require.NoError},
		{"003",
			[]entity.LimitHashEntity{
				{UserID: "test_user_003_6I5rvpdyB0", Hash: "hash_6I5rvpdyB0", Amount: 2002, Data: []byte("6I5rvpdyB0")},
			},
			require.NoError},
	}

	repo := NewRepository(app.NoOpLogger(), false)

	err := repo.Connect(mctx, app.Clickhouse{DSN: CLICKHOUSE_DSN})
	require.NoError(t, err)
	defer repo.conn.Close()

	for _, tc := range testcases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := repo.get()
			b.data = append(b.data, tc.hashes...)

			err := repo.save(mctx, b)
			tc.errAssertion(t, err)
			if err == nil {
				for _, h := range tc.hashes {
					exist := hashExists(t, repo.conn, h)
					require.True(t, exist)
				}
			}
		})
	}
}

func hashExists(t *testing.T, conn driver.Conn, hash entity.LimitHashEntity) bool {
	q := `SELECT COUNT(*) 
			FROM limits_hashes 
			WHERE user_id = ? 
			AND amount = ?
			AND sha256sum = ?`

	var cnt uint64
	err := conn.QueryRow(mctx, q, hash.UserID, hash.Amount, hash.Hash).Scan(&cnt)
	require.NoError(t, err)

	return cnt == 1
}

func produceLimits(count int, dataCh chan<- entity.LimitHashEntity, sleepInterval time.Duration) {
	for i := 1; i <= count; i++ {
		ent := entity.LimitHashEntity{
			UserID: "test_user_001",
			Hash:   strconv.Itoa(i),
			Data:   nil,
			Amount: uint32(i),
		}

		dataCh <- ent
		time.Sleep(time.Microsecond + time.Duration(rand.Int63n(int64(sleepInterval))))
	}
	// check that worker has read the entire buffer
	for {
		if len(dataCh) == 0 {
			break
		}
	}

	time.Sleep(time.Second)
}

func checkCount(t *testing.T, conn driver.Conn) (count uint64) {
	t.Helper()
	err := conn.QueryRow(mctx, `SELECT COUNT(*) FROM limits_hashes`).Scan(&count)
	require.NoError(t, err)

	return
}

func clearLimitsHashes(t *testing.T, conn driver.Conn) {
	t.Helper()
	err := conn.Exec(context.TODO(), `DELETE FROM limits_hashes WHERE user_id = 'test_user_001'`)
	require.NoError(t, err)
}

func newClickHouse() (*sql.DB, func(), error) {
	// Uses a sensible default on windows (tcp/http) and linux/osx (socket).
	pool, err := dockertest.NewPool("")
	if err != nil {
		return nil, nil, err
	}
	runOptions := &dockertest.RunOptions{
		Repository: CLICKHOUSE_IMAGE,
		Tag:        CLICKHOUSE_VERSION,
		Env: []string{
			"CLICKHOUSE_DB=" + CLICKHOUSE_DB,
			"CLICKHOUSE_USER=" + CLICKHOUSE_USER,
			"CLICKHOUSE_PASSWORD=" + CLICKHOUSE_PASSWORD,
			"CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=" + CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT,
		},
		Labels:       map[string]string{"goose_test": "1"},
		PortBindings: make(map[docker.Port][]docker.PortBinding),
	}
	runOptions.PortBindings[docker.Port("9000/tcp")] = []docker.PortBinding{
		{HostPort: strconv.Itoa(9001)},
	}
	container, err := pool.RunWithOptions(
		runOptions,
		func(config *docker.HostConfig) {
			// Set AutoRemove to true so that stopped container goes away by itself.
			config.AutoRemove = true
			config.RestartPolicy = docker.RestartPolicy{Name: "no"}
		},
	)
	if err != nil {
		return nil, nil, err
	}
	cleanup := func() {
		if err := pool.Purge(container); err != nil {
			log.Printf("failed to purge resource: %v", err)
		}
	}
	// Fetch port assigned to container
	address := fmt.Sprintf("%s:%s", "localhost", container.GetPort("9000/tcp"))

	var db *sql.DB
	// Exponential backoff-retry, because the application in the container
	// might not be ready to accept connections yet.
	if err := pool.Retry(func() error {
		db = clickHouseOpenDB(address, nil, false)
		return db.Ping()
	}); err != nil {
		return nil, cleanup, fmt.Errorf("could not connect to docker database: %w", err)
	}
	return db, cleanup, nil
}

func clickHouseOpenDB(address string, tlsConfig *tls.Config, debug bool) *sql.DB {
	db := clickhouse.OpenDB(&clickhouse.Options{
		Addr: []string{address},
		Auth: clickhouse.Auth{
			Database: CLICKHOUSE_DB,
			Username: CLICKHOUSE_USER,
			Password: CLICKHOUSE_PASSWORD,
		},
		TLS: tlsConfig,
		Settings: clickhouse.Settings{
			"max_execution_time": 60,
		},
		DialTimeout: 5 * time.Second,
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
		Debug: debug,
	})
	db.SetMaxIdleConns(5)
	db.SetMaxOpenConns(10)
	db.SetConnMaxLifetime(time.Hour)
	return db
}
