package api

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

func Interact(ctx context.Context, tasks common.Tasklist) chan struct{} {
	gin.SetMode(gin.ReleaseMode)

	router := gin.New()
	router.Use(requestLogger())
	router.Use(gin.Recovery())

	api := router.Group("api")

	withTaskList := func(fn func(*gin.Context, common.Tasklist)) func(c *gin.Context) {
		return func(c *gin.Context) {
			fn(c, tasks)
		}
	}

	v1 := api.Group("/v1")
	{
		v1.POST("/tasks", withTaskList(postTask))
	}

	srv := &http.Server{
		Addr:    ":8080",
		Handler: router,
	}

	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error().Str("mod", mod).Err(err).Send()
		}

	}()

	ch := make(chan struct{})
	go func() {
		<-ctx.Done()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		err := srv.Shutdown(ctx)
		if err == nil {
			log.Info().Str("mod", mod).Msg("shutdown")
		} else {
			log.Error().Str("mod", mod).Err(err).Send()
		}
		close(ch)
	}()

	return ch
}

func postTask(c *gin.Context, tasks common.Tasklist) {
	fileHeader, _ := c.FormFile("file")
	file, _ := fileHeader.Open()
	bytesArr, _ := ioutil.ReadAll(file)
	rawTask := common.RawTask{
		Description: string(bytesArr),
		ScheduledAt: c.PostForm("scheduled_at"),
	}

	err := tasks.Write(c.Request.Context(), rawTask)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": err.Error(),
		})
		return
	}
	c.JSON(http.StatusOK, gin.H{})
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
