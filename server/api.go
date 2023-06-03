package server

import (
	"context"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/rs/zerolog/log"
	"github.com/turnon/clams/tasklist/common"
)

const mod = "api"

type ApplicationInterface struct {
	ch    chan struct{}
	ctx   context.Context
	tasks common.Tasklist
}

func newApi(ctx context.Context, tasks common.Tasklist) *ApplicationInterface {
	api := &ApplicationInterface{ctx: ctx, tasks: tasks}
	api.start()
	return api
}

// wait 等待worker退出
func (api *ApplicationInterface) wait() chan struct{} {
	return api.ch
}

// logErr 输出日志
func (api *ApplicationInterface) logErr(err error) {
	log.Error().Str("mod", "api").Err(err).Send()
}

func (api *ApplicationInterface) start() {
	api.ch = make(chan struct{})

	gin.SetMode(gin.ReleaseMode)

	router := gin.New()
	router.Use(requestLogger())
	router.Use(gin.Recovery())

	path := router.Group("api")

	v1 := path.Group("/v1")
	{
		v1.POST("/tasks", api.postTasks)
		v1.DELETE("/tasks/:id", api.deleteTasks)
	}

	httpSrv := &http.Server{
		Addr:    ":8080",
		Handler: router,
	}

	go func() {
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			api.logErr(err)
		}
	}()

	go func() {
		<-api.ctx.Done()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		err := httpSrv.Shutdown(ctx)
		if err == nil {
			log.Info().Str("mod", mod).Msg("shutdown")
		} else {
			api.logErr(err)
		}
		close(api.ch)
	}()
}

// postTasks 新建任务
func (api *ApplicationInterface) postTasks(c *gin.Context) {
	fileHeader, _ := c.FormFile("file")
	file, _ := fileHeader.Open()
	bytesArr, _ := ioutil.ReadAll(file)
	rawTask := common.RawTask{
		Description: string(bytesArr),
		ScheduledAt: c.PostForm("scheduled_at"),
	}

	err := api.tasks.Write(c.Request.Context(), rawTask)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": err.Error(),
		})
		return
	}
	c.JSON(http.StatusOK, gin.H{})
}

// deleteTasks 删除任务
func (api *ApplicationInterface) deleteTasks(c *gin.Context) {
	id := c.Param("id")
	err := api.tasks.Delete(c.Request.Context(), id)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": err.Error(),
		})
		return
	}
	c.JSON(http.StatusNoContent, gin.H{})
}

func requestLogger() gin.HandlerFunc {
	return func(ctx *gin.Context) {
		startTime := time.Now()
		ctx.Next()
		log.
			Info().
			Str("mod", mod).
			Int("code", ctx.Writer.Status()).
			Str("method", ctx.Request.Method).
			Str("path", ctx.Request.RequestURI).
			TimeDiff("latency", time.Now(), startTime).
			Send()
	}
}
