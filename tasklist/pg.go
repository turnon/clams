package tasklist

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// initPgTasklist 初始化pgTaskList
func initPgTasklist(ctx context.Context, cfg map[string]any) (*pgTaskList, error) {
	// url := cfg["url"].(string)
	url := "postgresql://root:mysecretpassword@localhost:5432/clams"
	conn, err := pgxpool.New(ctx, url)
	if err != nil {
		return nil, err
	}

	list := pgTaskList{
		ctx:   ctx,
		conn:  conn,
		tasks: make(chan task),
	}
	if err := list.init(ctx); err != nil {
		return nil, err
	}

	go list.loopFetch()

	return &list, nil
}

// pgTaskList 可从pg读写任务
type pgTaskList struct {
	ctx   context.Context
	conn  *pgxpool.Pool
	tasks chan task
}

// init 初始化pg任务列表
func (list *pgTaskList) init(ctx context.Context) error {
	_, err := list.conn.Exec(ctx, `
	create table if not exists tasks(
		id SERIAL PRIMARY KEY,
		description TEXT,
		created_at TIMESTAMP,
		scheduled_at TIMESTAMP,
		performed_at TIMESTAMP,
		finished_at TIMESTAMP,
		error TEXT
	)`)
	if err != nil {
		return err
	}
	return nil
}

// Close 断开pg任务列表
func (list *pgTaskList) Close(ctx context.Context) error {
	list.conn.Close()
	return nil
}

// Read 从pg读出一个任务
func (list *pgTaskList) Read() chan task {
	return list.tasks
}

// Read 往pg写入一个任务
func (list *pgTaskList) Write(ctx context.Context, rawTask RawTask) error {
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
	for {
		select {
		case <-list.ctx.Done():
			return
		case <-time.After(2 * time.Second):
		}

		oneTask, err := list.fetchOne()
		if errors.Is(err, pgx.ErrNoRows) || oneTask == nil {
			continue
		}
		if err != nil {
			continue
		}

		select {
		case <-list.ctx.Done():
			return
		case list.tasks <- oneTask:
		}
	}
}

// fetchOne 从pg读出一个任务
func (list *pgTaskList) fetchOne() (task, error) {
	t := &pgTask{list: list}

	sql := `
	select id, description
	from tasks
	where performed_at is null
	and finished_at is null
	order by scheduled_at
	limit 1`

	err := list.conn.AcquireFunc(list.ctx, func(c *pgxpool.Conn) error {
		return c.QueryRow(list.ctx, sql).Scan(&t.id, &t.description)
	})
	if err != nil {
		return nil, err
	}

	// lock
	var lockable bool
	err = list.conn.AcquireFunc(list.ctx, func(c *pgxpool.Conn) error {
		return c.QueryRow(list.ctx, "select pg_try_advisory_lock($1)", t.id).Scan(&lockable)
	})
	if err != nil {
		return nil, err
	}
	if !lockable {
		return nil, nil
	}
	defer list.conn.AcquireFunc(list.ctx, func(c *pgxpool.Conn) error {
		c.QueryRow(list.ctx, "select pg_advisory_unlock($1)", t.id)
		return nil
	})

	// mark performing
	res, err := list.conn.Exec(list.ctx, "update tasks set performed_at = $1 where id = $2 and performed_at is null", time.Now(), t.id)
	if err != nil {
		return nil, err
	}
	if res.RowsAffected() != 1 {
		return nil, nil
	}

	return t, nil
}

// pgTask 代表一个任务
type pgTask struct {
	list        *pgTaskList
	id          int
	description string
}

// Description 返回任务脚本
func (t *pgTask) Description() string {
	return t.description
}

// Done 标记任务结束
func (t *pgTask) Done(ctx context.Context) error {
	_, err := t.list.conn.Exec(t.list.ctx, "update tasks set finished_at = $1 where id = $2", time.Now(), t.id)
	return err
}

// Done 标记任务错误
func (t *pgTask) Error(ctx context.Context, err error) error {
	_, updateErr := t.list.conn.Exec(t.list.ctx, "update tasks set finished_at = $1,  error = $2 where id = $3", time.Now(), err.Error(), t.id)
	return updateErr
}
