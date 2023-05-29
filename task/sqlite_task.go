package task

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/mattn/go-sqlite3"
)

type sqliteTask struct {
	id int
}

func (t *sqliteTask) Connect(ctx context.Context) {

}

func (t *sqliteTask) Load(ctx context.Context) (string, error) {
	db, err := sql.Open("sqlite3", "file:foo.db?_locking=EXCLUSIVE")
	if err != nil {
		return "", err
	}
	defer db.Close()

	sqlStmt := `create table if not exists jobs (id integer not null primary key, task text, created_at text, created_by text);`
	_, err = db.Exec(sqlStmt)
	if err != nil {
		return "", err
	}

	task, err := t.loadJob(db)
	if err != nil {
		return "", err
	}

	if task == "" {
		return "", err
	}

	fmt.Println(task)

	t.markProcessing(db)

	return task, nil
}

func (t *sqliteTask) loadJob(db *sql.DB) (string, error) {
	var (
		id   int
		task string
	)
	rows, err := db.Query("select id, task from jobs order by created_at limit 1")
	if err != nil {
		return "", err
	}
	defer rows.Close()

	for rows.Next() {
		err = rows.Scan(&id, &task)
		if err != nil {
			return "", err
		}
	}
	err = rows.Err()
	if err != nil {
		return "", err
	}

	t.id = id

	return task, nil
}

func (t *sqliteTask) markProcessing(db *sql.DB) error {
	return nil
}

func (t *sqliteTask) Done(ctx context.Context) error {
	return nil
}
