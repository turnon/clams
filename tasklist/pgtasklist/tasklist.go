package pgtasklist

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog/log"
	"github.com/turnon/clams/tasklist/common"
)

const tasksChannel = "tasks_channel"

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
		localTasks: newLocalcache(),
	}
	if err := list.init(ctx); err != nil {
		return nil, err
	}

	go list.loopFetch()
	go list.listenForAbort()

	return &list, nil
}

// pgTaskList 可从pg读写任务
type pgTaskList struct {
	ctx         context.Context
	conn        *pgxpool.Pool
	localTasks  *localcache
	readerCount int
}

// debugf 打印调试信息
func (list *pgTaskList) debugf(str string, v ...any) {
	log.Debug().Str("mod", "tasklist").Msgf(str, v...)
}

// errorf 打印错误信息
func (list *pgTaskList) errorf(str string, v ...any) {
	log.Error().Str("mod", "tasklist").Msgf(str, v...)
}

// init 初始化pg任务列表
func (list *pgTaskList) init(ctx context.Context) error {
	_, err := list.conn.Exec(ctx, `
	create table if not exists tasks (
		id SERIAL PRIMARY KEY,
		created_at TIMESTAMP,
		scheduled_at TIMESTAMP,
		performed_at TIMESTAMP,
		finished_at TIMESTAMP,
		cancelled_at TIMESTAMP,
		description TEXT,
		error TEXT
	)`)
	if err != nil {
		return err
	}

	return nil
}

// listenForAbort 监听任务中止
func (list *pgTaskList) listenForAbort() {
	abortSignal := make(chan struct{}, 1)

	go func() {
		list.conn.AcquireFunc(list.ctx, func(c *pgxpool.Conn) error {
			_, listenErr := c.Exec(list.ctx, "listen "+tasksChannel)
			if listenErr != nil {
				list.errorf("list: %v", listenErr)
				close(abortSignal)
			}

			for {
				_, waitErr := c.Conn().WaitForNotification(list.ctx)
				if errors.Is(waitErr, context.Canceled) {
					return nil
				}
				if waitErr != nil {
					list.errorf("WaitForNotification: %v", waitErr)
					close(abortSignal)
					return waitErr
				}

				select {
				case abortSignal <- struct{}{}:
				default:
				}
			}
		})
	}()

	go func() {
		for {
			select {
			case <-list.ctx.Done():
				return
			case <-abortSignal:
				funcErr := list.conn.AcquireFunc(list.ctx, func(c *pgxpool.Conn) error {
					ids := list.localTasks.getIds()
					if len(ids) == 0 {
						return nil
					}

					sql := "select id from tasks where cancelled_at is not null and id = any($1)"
					rows, queryErr := c.Query(list.ctx, sql, ids)
					if queryErr != nil {
						return queryErr
					}
					defer rows.Close()

					for rows.Next() {
						var id int
						if scanErr := rows.Scan(&id); scanErr != nil {
							return scanErr
						}
						list.localTasks.del(id)
					}

					return nil
				})
				if funcErr != nil {
					list.errorf("loop abortSignal: %v", funcErr)
					return
				}
			}
		}
	}()
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

// Delete 删除任务
func (list *pgTaskList) Delete(ctx context.Context, idStr string) error {
	idInt, idErr := strconv.Atoi(idStr)
	if idErr != nil {
		return idErr
	}

	var cancelErr error
	lockErr := list.lock(ctx, idInt, func(ctx context.Context, id int) {
		sql := `
		update tasks
		set cancelled_at = $1
		where id = $2
		and cancelled_at is null
		and finished_at is null
		`

		_, cancelErr = list.conn.Exec(ctx, sql, time.Now(), id)
		if cancelErr != nil {
			return
		}

		list.conn.Exec(context.Background(), "select pg_notify('"+tasksChannel+"', $1)", "abort")
	})
	if lockErr != nil {
		return lockErr
	}

	return cancelErr
}

// Write 往pg写入一个任务
func (list *pgTaskList) Write(ctx context.Context, rawTask common.RawTask) error {
	scheduledAt := rawTask.ScheduledAt
	if scheduledAt == "" {
		scheduledAt = time.Now().Format("2006-01-02 15:04:05")
	}

	sql := "insert into tasks (description, created_at, scheduled_at) values ($1, $2, $3)"
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
	and scheduled_at <= current_timestamp
	and finished_at is null
	and cancelled_at is null
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

			list.localTasks.init(ids...)
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
		var (
			t        common.Task
			fetchErr error
		)
		lockErr := list.lock(ctx, id, func(ctx context.Context, id int) {
			t, fetchErr = list._fetchOne(ctx, id)
		})

		list.debugf("_fetchOne %v: %p, %v", id, t, fetchErr)
		if lockErr != nil || errors.Is(fetchErr, pgx.ErrNoRows) {
			continue
		}
		return t, fetchErr
	}

	return nil, pgx.ErrNoRows
}

// lock 加锁
func (list *pgTaskList) lock(ctx context.Context, id int, fn func(context.Context, int)) error {
	var lockable bool
	err := list.conn.AcquireFunc(ctx, func(c *pgxpool.Conn) error {
		return c.QueryRow(ctx, "select pg_try_advisory_lock($1)", id).Scan(&lockable)
	})
	if err != nil {
		return err
	}
	if !lockable {
		return nil
	}
	defer list.conn.AcquireFunc(ctx, func(c *pgxpool.Conn) error {
		c.QueryRow(ctx, "select pg_advisory_unlock($1)", id)
		return nil
	})

	fn(ctx, id)
	return nil
}

// _fetchOne 从pg读出一个任务
func (list *pgTaskList) _fetchOne(ctx context.Context, id int) (common.Task, error) {
	var t *pgTask
	err := list.conn.AcquireFunc(ctx, func(c *pgxpool.Conn) error {
		markPerforming := `
		update tasks
		set performed_at = $1
		where id = $2
		and scheduled_at <= current_timestamp
		and performed_at is null
		and cancelled_at is null
		returning description
		`

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

		t = &pgTask{
			id:          id,
			list:        list,
			description: desc,
			aborted:     make(chan struct{}),
		}
		list.localTasks.set(id, t)
		return nil
	})

	return t, err
}
