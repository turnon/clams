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

	go func() {
		for {
			t := &pgTask{description: "123"}
			list.tasks <- t
			<-time.After(2 * time.Second)
		}
	}()

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

// // Fetch 从pg读出一个任务
// func (list *pgTaskList) fetch() chan task {
// 	for {
// 		select{
// 			case: list.ctx.
// 		}
// 	}
// 	return list
// }

// pgTask 代表一个任务
type pgTask struct {
	list        *pgTaskList
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
