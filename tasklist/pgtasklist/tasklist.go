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
		ctx:          ctx,
		conn:         conn,
		readyTaskIds: make(chan int),
		runningTasks: newLocalcache(),
	}
	if err := list.init(ctx); err != nil {
		return nil, err
	}

	go list.loopFindTasks()
	go list.listenForAbort()

	return &list, nil
}

// pgTaskList 可从pg读写任务
type pgTaskList struct {
	ctx          context.Context
	conn         *pgxpool.Pool
	readyTaskIds chan int
	runningTasks *localcache
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
				if err := list.abortTasks(); err != nil {
					list.errorf("loop abortSignal: %v", err)
					return
				}
			}
		}
	}()
}

// abortTasks 中止运行中的任务
func (list *pgTaskList) abortTasks() error {
	return list.conn.AcquireFunc(list.ctx, func(c *pgxpool.Conn) error {
		ids := list.runningTasks.getIds()
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
			list.runningTasks.del(id)
		}

		return nil
	})
}

// Read 返回一个任务
func (list *pgTaskList) Read(ctx context.Context) (common.Task, error) {
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case id := <-list.readyTaskIds:
			t, err := list.fetchOne(ctx, id)
			if err == nil || err == context.Canceled {
				return t, err
			}
		}
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
		scheduledAt = list.timeNowStr()
	}

	sql := "insert into tasks (description, created_at, scheduled_at) values ($1, $2, $3)"
	_, err := list.conn.Exec(ctx, sql, rawTask.Description, time.Now(), scheduledAt)
	if err != nil {
		return err
	}
	return nil
}

// loopFindTasks 从pg轮询任务
func (list *pgTaskList) loopFindTasks() {
	for {
		err := list.fetchSomeIds()
		list.debugf("loopFindTasks %v", err)

		if errors.Is(err, pgx.ErrNoRows) {
			<-time.After(1 * time.Minute)
			continue
		}
		if errors.Is(err, context.Canceled) {
			return
		}
	}
}

// fetchSomeIds 取出一些可执行的id
func (list *pgTaskList) fetchSomeIds() error {
	sql := `
	select id
	from tasks
	where performed_at is null
	and scheduled_at <= $1
	and finished_at is null
	and cancelled_at is null
	order by scheduled_at
	limit 10`

	idSet := make(map[int]struct{})
	funcErr := list.conn.AcquireFunc(list.ctx, func(c *pgxpool.Conn) error {
		rows, queryErr := c.Query(list.ctx, sql, list.timeNowStr())
		if queryErr != nil {
			return queryErr
		}
		defer rows.Close()

		for rows.Next() {
			var id int
			if scanErr := rows.Scan(&id); scanErr != nil {
				return scanErr
			}
			idSet[id] = struct{}{}
		}
		return nil
	})
	if funcErr != nil {
		return funcErr
	}

	if len(idSet) == 0 {
		return pgx.ErrNoRows
	}

	maybeOutdated := time.After(1 * time.Minute)
	for id := range idSet {
		select {
		case <-list.ctx.Done():
			return list.ctx.Err()
		case <-maybeOutdated:
			return nil
		case list.readyTaskIds <- id:
		}
	}

	return nil
}

// fetchOne 从pg读出一个任务
func (list *pgTaskList) fetchOne(ctx context.Context, id int) (common.Task, error) {
	var (
		t        common.Task
		fetchErr error
	)
	lockErr := list.lock(ctx, id, func(ctx context.Context, id int) {
		t, fetchErr = list._fetchOne(ctx, id)
	})
	if lockErr != nil {
		return nil, lockErr
	}
	if fetchErr != nil {
		return nil, fetchErr
	}
	return t, fetchErr
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
		return pgx.ErrNoRows
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
	fnErr := list.conn.AcquireFunc(ctx, func(c *pgxpool.Conn) error {
		markPerforming := `
		update tasks
		set performed_at = $1
		where id = $2
		and scheduled_at <= $3
		and performed_at is null
		and cancelled_at is null
		returning description
		`

		now := list.timeNowStr()
		rows, err := c.Query(ctx, markPerforming, now, id, now)
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
		list.runningTasks.set(id, t)
		return nil
	})

	return t, fnErr
}

// timeNowStr 当前时间
func (list *pgTaskList) timeNowStr() string {
	return time.Now().Format("2006-01-02 15:04:05")
}
