package pgtasklist

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog/log"
	"github.com/turnon/clams/tasklist/common"
)

// Init 初始化pgTaskList
func Init(ctx context.Context, cfg map[string]any) (*pgTaskList, error) {
	url := cfg["url"].(string)
	conn, err := pgxpool.New(ctx, url)
	if err != nil {
		return nil, err
	}

	list := pgTaskList{
		ctx:        ctx,
		conn:       conn,
		tasksCache: newLocalcache(),
	}
	if err := list.init(ctx); err != nil {
		return nil, err
	}

	go list.loopFetch()

	return &list, nil
}

// pgTaskList 可从pg读写任务
type pgTaskList struct {
	ctx         context.Context
	conn        *pgxpool.Pool
	tasksCache  *localcache
	readerCount int
}

// debugf 打印调试信息
func (list *pgTaskList) debugf(str string, v ...any) {
	log.Debug().Str("mod", "tasklist").Msgf(str, v...)
}

// init 初始化pg任务列表
func (list *pgTaskList) init(ctx context.Context) error {
	_, err := list.conn.Exec(ctx, `
	create table if not exists tasks(
		id SERIAL PRIMARY KEY,
		created_at TIMESTAMP,
		scheduled_at TIMESTAMP,
		performed_at TIMESTAMP,
		finished_at TIMESTAMP,
		description TEXT,
		error TEXT
	)`)
	if err != nil {
		return err
	}
	return nil
}

// NewReader 返回一个Reader
func (list *pgTaskList) NewReader() common.TaskReader {
	list.readerCount += 1
	return &pgTaskReader{
		idx:  list.readerCount,
		list: list,
	}
}

// Close 断开pg任务列表
func (list *pgTaskList) Close(ctx context.Context) error {
	list.conn.Close()
	return nil
}

// Write 往pg写入一个任务
func (list *pgTaskList) Write(ctx context.Context, rawTask common.RawTask) error {
	scheduledAt := rawTask.ScheduledAt
	if scheduledAt == "" {
		scheduledAt = time.Now().Format("2006-01-02 15:04:05")
	}

	sql := "insert into tasks(description, created_at, scheduled_at) values ($1, $2, $3)"
	_, err := list.conn.Exec(ctx, sql, rawTask.Description, time.Now(), scheduledAt)
	if err != nil {
		return err
	}
	return nil
}

// loopFetch 从pg轮询任务
func (list *pgTaskList) loopFetch() {
	ticker := time.NewTicker(10 * time.Second)

	sql := `
	select id
	from tasks
	where performed_at is null
	and finished_at is null
	order by scheduled_at
	limit 10`

	for {
		funcErr := list.conn.AcquireFunc(list.ctx, func(c *pgxpool.Conn) error {
			rows, queryErr := c.Query(list.ctx, sql)
			if queryErr != nil {
				return queryErr
			}
			defer rows.Close()

			ids := []int{}
			for rows.Next() {
				var id int
				if scanErr := rows.Scan(&id); scanErr != nil {
					return scanErr
				}
				ids = append(ids, id)
			}

			list.tasksCache.set(ids...)
			list.debugf("loopFetch %v", ids)

			return nil
		})
		if funcErr != nil {
			list.debugf("loopFetch err: %v", funcErr)
		}

		select {
		case <-list.ctx.Done():
			list.debugf("loopFetch end")
			return
		case <-ticker.C:
		}
	}
}

// fetchOne 从pg读出一个任务
func (list *pgTaskList) fetchOne(ctx context.Context, ids ...int) (common.Task, error) {
	for _, id := range ids {
		t, err := list._fetchOne(ctx, id)
		list.debugf("_fetchOne %v: %p, %v", id, t, err)
		if errors.Is(err, pgx.ErrNoRows) {
			continue
		}
		return t, err
	}
	return nil, pgx.ErrNoRows
}

// _fetchOne 从pg读出一个任务
func (list *pgTaskList) _fetchOne(ctx context.Context, id int) (common.Task, error) {
	// lock
	var lockable bool
	err := list.conn.AcquireFunc(ctx, func(c *pgxpool.Conn) error {
		return c.QueryRow(ctx, "select pg_try_advisory_lock($1)", id).Scan(&lockable)
	})
	if err != nil {
		return nil, err
	}
	if !lockable {
		return nil, nil
	}
	defer list.conn.AcquireFunc(ctx, func(c *pgxpool.Conn) error {
		c.QueryRow(ctx, "select pg_advisory_unlock($1)", id)
		return nil
	})

	var t *pgTask
	err = list.conn.AcquireFunc(ctx, func(c *pgxpool.Conn) error {
		markPerforming := "update tasks set performed_at = $1 where id = $2 and performed_at is null returning description"
		rows, err := c.Query(ctx, markPerforming, time.Now(), id)
		if err != nil {
			return err
		}

		var desc string
		for rows.Next() {
			if err = rows.Scan(&desc); err != nil {
				return err
			}
		}
		if desc == "" {
			return pgx.ErrNoRows
		}

		list.tasksCache.del(id)
		t = &pgTask{id: id, list: list, description: desc}
		return nil
	})

	return t, err
}
