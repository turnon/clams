package tasklist

import (
	"context"
	"errors"

	"github.com/turnon/clams/tasklist/common"
	"github.com/turnon/clams/tasklist/pgtasklist"
)

func NewTaskList(ctx context.Context, cfg map[string]any) (common.Tasklist, error) {
	if cfg["type"] == "pg" {
		return pgtasklist.Init(ctx, cfg)
	}
	return nil, errors.New("no tasklist config")
}
