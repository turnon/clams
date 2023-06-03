package pgtasklist

import (
	"context"
	"strconv"
	"time"
)

// pgTask 代表一个任务
type pgTask struct {
	list        *pgTaskList
	id          int
	description string
	aborted     chan struct{}
}

// Description 返回任务id
func (t *pgTask) ID() string {
	return strconv.Itoa(t.id)
}

// Description 返回任务脚本
func (t *pgTask) Description() string {
	return t.description
}

// Aborted 监听中止
func (t *pgTask) Aborted() chan struct{} {
	return t.aborted
}

// Done 标记任务结束
func (t *pgTask) Done(ctx context.Context) error {
	_, err := t.list.conn.Exec(ctx, "update tasks set finished_at = $1 where id = $2", time.Now(), t.id)
	return err
}

// Done 标记任务错误
func (t *pgTask) Error(ctx context.Context, err error) error {
	sql := "update tasks set finished_at = $1, error = $2 where id = $3"
	_, updateErr := t.list.conn.Exec(ctx, sql, time.Now(), err.Error(), t.id)
	return updateErr
}
