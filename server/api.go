package server

import (
	"context"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/rs/zerolog/log"
	"github.com/turnon/clams/tasklist/common"
)

const mod = "api"

type ApplicationInterface struct {
	port  int
	ch    chan struct{}
	ctx   context.Context
	tasks common.Tasklist
}

func newApi(ctx context.Context, port int, tasks common.Tasklist) *ApplicationInterface {
	api := &ApplicationInterface{ctx: ctx, port: port, tasks: tasks}
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
	if api.port == 0 {
		api.port = 80
	}

	gin.SetMode(gin.ReleaseMode)

	router := gin.New()
	router.Use(requestLogger())
	router.Use(gin.Recovery())

	path := router.Group("api")

	v1 := path.Group("/v1")
	{
		v1.POST("/tasks", api.postTasks)
		v1.DELETE("/tasks/:id", api.deleteTasks)
		v1.GET("/tasks/:id", api.getTasks)
	}

	httpSrv := &http.Server{
		Addr:    ":" + strconv.Itoa(api.port),
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
	bytesArr, _ := io.ReadAll(file)
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

// getTasks 查看任务
func (api *ApplicationInterface) getTasks(c *gin.Context) {
	id := c.Param("id")
	t, err := api.tasks.Peek(c.Request.Context(), id)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": err.Error(),
		})
		return
	}

	c.Header("Content-Disposition", "attachment; filename="+id+".yml")
	c.Header("Content-Type", "application/octet-stream")
	c.Writer.Write([]byte(t.Description))
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
