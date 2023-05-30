package tasklist

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
)

// initPgTasklist 初始化pgTaskList
func initPgTasklist(ctx context.Context, cfg map[string]any) (*pgTaskList, error) {
	// url := cfg["url"].(string)
	url := "postgresql://root:mysecretpassword@localhost:5432/clams"
	conn, err := pgx.Connect(ctx, url)
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
	conn  *pgx.Conn
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
	return list.conn.Close(ctx)
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

	err := list.conn.QueryRow(context.Background(), sql).Scan(&t.id, &t.description)
	if err != nil {
		return nil, err
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
	return nil
}
