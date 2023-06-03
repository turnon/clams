package server

import (
	"context"
	"io/ioutil"
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
}

// mainServer 主服务器
type mainServer struct {
	cfg *config
}

// subordinate 从服务器
type subordinate interface {
	wait() chan struct{}
}

// Run 根据配置启动服务器
func Run(cfgPath string) {
	bytesArr, err := ioutil.ReadFile(cfgPath)
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

	srv := mainServer{cfg: &cfg}
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
	children := make([]subordinate, 0, 1+srv.cfg.Workers)
	children = append(children, newApi(sigCtx, tasks))
	for i := 0; i < srv.cfg.Workers; i++ {
		children = append(children, newTaskWorker(sigCtx, i, tasks))
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
