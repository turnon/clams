package pgtasklist

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/rs/zerolog/log"
	"github.com/turnon/clams/tasklist/common"
)

type pgTaskReader struct {
	list *pgTaskList
	idx  int
}

// debugf 输出日志
func (reader *pgTaskReader) debugf(str string, v ...any) {
	log.Debug().Str("mod", "pgTaskReader").Int("idx", reader.idx).Msgf(str, v...)
}

// infof 输出日志
func (reader *pgTaskReader) infof(str string, v ...any) {
	log.Info().Str("mod", "pgTaskReader").Int("idx", reader.idx).Msgf(str, v...)
}

// Read 读取一个任务
func (reader *pgTaskReader) Read(ctx context.Context) (common.Task, error) {
	defer reader.infof("return")

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		ids := reader.list.tasksCache.get()
		reader.debugf("get %v", ids)

		if len(ids) == 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(10 * time.Second):
				continue
			}
		}

		t, err := reader.list.fetchOne(ctx, ids...)
		reader.debugf("fetchOne %p, %v", t, err)

		if errors.Is(err, pgx.ErrNoRows) || t == nil {
			continue
		}

		return t, err
	}
}
