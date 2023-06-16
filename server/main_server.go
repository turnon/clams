package server

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/rs/zerolog/log"

	"github.com/turnon/clams/tasklist"

	"gopkg.in/yaml.v3"
)

// config 服务器配置
type config struct {
	Tasklist map[string]any `yaml:"tasklist"`
	Workers  int            `yaml:"workers"`
	Port     int            `yaml:"port"`
}

// mainServer 主服务器
type mainServer struct {
	cfg     *config
	anchors string
}

// subordinate 从服务器
type subordinate interface {
	wait() chan struct{}
}

// Run 根据配置启动服务器
func Run(anchors string, cfgPath string) {
	bytesArr, err := os.ReadFile(cfgPath)
	if err != nil {
		panic(err)
	}

	var cfg config
	err = yaml.Unmarshal(bytesArr, &cfg)
	if err != nil {
		panic(err)
	}

	if cfg.Workers <= 0 {
		cfg.Workers = 1
	}

	srv := mainServer{cfg: &cfg, anchors: anchors}
	<-srv.run()
}

// run 运行主服务器和从服务器
func (srv *mainServer) run() chan struct{} {
	ch := make(chan struct{})
	sigCtx, _ := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)

	// 连接任务列表
	tasks, err := tasklist.NewTaskList(sigCtx, srv.cfg.Tasklist)
	if err != nil {
		log.Error().Str("mod", "server").Msgf("NewTaskList err: %v", err)
		close(ch)
		return ch
	}

	// 运行从服务器
	children := []subordinate{
		newApi(sigCtx, srv.cfg.Port, tasks),
		newWorkteam(sigCtx, tasks, srv.cfg.Workers, srv.anchors),
	}

	// 等待从服务器退出
	go func() {
		for _, child := range children {
			<-child.wait()
		}
		close(ch)
	}()

	return ch
}
