package api

import (
	"context"
	"io/ioutil"
	"log"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/turnon/clams/tasklist"
)

func Interact(ctx context.Context, tasks tasklist.Tasklist) chan struct{} {
	gin.SetMode(gin.ReleaseMode)
	router := gin.Default()
	api := router.Group("api")

	withTaskList := func(fn func(*gin.Context, tasklist.Tasklist)) func(c *gin.Context) {
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
			log.Fatal("api listen error:", err)
		}

	}()

	ch := make(chan struct{})
	go func() {
		<-ctx.Done()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		err := srv.Shutdown(ctx)
		if err == nil {
			log.Println("api shutdown ok")
		} else {
			log.Fatal("api shutdown error:", err)
		}
		close(ch)
	}()

	return ch
}

func postTask(c *gin.Context, tasks tasklist.Tasklist) {
	fileHeader, _ := c.FormFile("file")
	file, _ := fileHeader.Open()
	bytesArr, _ := ioutil.ReadAll(file)
	rawTask := tasklist.RawTask{
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
