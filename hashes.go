package ch

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

func (repo *Repository) SaveAsync(_ context.Context, hashEntity any) {
	if !repo.enabled {
		// repo.logger.Debug("ch currently disabled, skip message")
		return
	}

	repo.dataChan <- hashEntity
}

//// альтернатива processLimitHashes.
// PS: производительность оказалась значительно ХУЖЕ чем у processLimitHashes.
//func (repo *Repository) asyncInsert(ctx context.Context, hashEntity entity.LimitHashEntity) {
//	defer metrics.SetDuration(time.Now(), "ch_hashes_batch_save")
//
//	query := "INSERT INTO limits_hashes (user_id, amount, msg, sha256sum) VALUES (?, ?, ?, ?)"
//
//	err := repo.conn.AsyncInsert(ctx, query, false, hashEntity.UserID, hashEntity.Amount, hashEntity.Data, hashEntity.Hash)
//	if err != nil {
//		repo.logger.Errorf("async insert hash: %v", err)
//	}
//}

// processLimitHashes накапливает батч строк и сохраняет их в зависимости от того, что случится раньше:
// произойдет тик тикера либо длина батча достигнет определенного лимита.
func (repo *Repository) processLimitHashes(ctx context.Context, interval time.Duration, maxBatchCap int) {
	cur := repo.get()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			go func(hb *batch) {
				if err := repo.save(ctx, cur); err != nil {
					repo.logger.Errorf("saving batch by tick: %s", err)
				}
			}(cur)

			cur = repo.get()
		case hashEntity := <-repo.dataChan:
			cur.data = append(cur.data, hashEntity)
			if len(cur.data) >= maxBatchCap {
				go func(hb *batch) {
					if err := repo.save(ctx, cur); err != nil {
						repo.logger.Errorf("saving batch by tick: %s", err)
					}
				}(cur)

				cur = repo.get()
			}
		}
	}
}

const maxBatchCap = 10_000

type batch struct {
	data []any
	// для идемпотентности метода save
	isSent atomic.Bool
}

func (repo *Repository) save(ctx context.Context, b *batch) error {
	defer repo.put(b)

	if len(b.data) == 0 || b.isSent.Load() {
		return nil
	}

	b.isSent.Store(true)
	defer repo.logger.Debugw("hashes batch saved", "len", len(b.data))

	connBatch, err := repo.conn.PrepareBatch(ctx, "INSERT INTO test_table (col1, col2, col3, col4) VALUES (?, ?, ?, ?)")
	if err != nil {
		return fmt.Errorf("preparing batch: %w", err)
	}

	for _ = range b.data {
		if err = connBatch.Append("col1", "col2", "col3", "col4"); err != nil {
			return fmt.Errorf("appending values: %w", err)
		}
	}

	if err = connBatch.Send(); err != nil {
		return fmt.Errorf("sending batch: %w", err)
	}

	return nil
}

func (repo *Repository) get() *batch {
	return repo.batchesPool.Get().(*batch) // nolint:forcetypeassert
}

func (repo *Repository) put(b *batch) {
	if cap(b.data) > maxBatchCap {
		return
	}

	b.data = b.data[:0]
	b.isSent.Store(false)
	repo.batchesPool.Put(b)
}

func initBatchPool() *sync.Pool {
	return &sync.Pool{New: func() any { return &batch{} }}
}
