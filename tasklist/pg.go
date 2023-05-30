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

	tasks := make(chan task)
	go func() {
		for {
			t := &pgTask{description: "123"}
			tasks <- t
			<-time.After(2 * time.Second)
		}
	}()

	list := pgTaskList{
		ctx:   ctx,
		conn:  conn,
		tasks: tasks,
	}
	return &list, nil
}

// pgTaskList 可从pg读写任务
type pgTaskList struct {
	ctx   context.Context
	conn  *pgx.Conn
	tasks chan task
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
	return nil
}

// // Fetch 从pg读出一个任务
// func (tl *pgTaskList) fetch(ctx context.Context) chan task {
// 	t <-
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
