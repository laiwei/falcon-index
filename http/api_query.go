package http

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/laiwei/falcon-index/index"
	"strconv"
)

func configApiQueryRoutes() {
	router.GET("/ping", func(c *gin.Context) {
		c.JSON(200, gin.H{
			"message": "pong",
		})
	})

	router.GET("/api/v1/field", func(c *gin.Context) {
		q := c.DefaultQuery("q", "")
		start := c.DefaultQuery("start", "")
		limit, err := strconv.Atoi(c.DefaultQuery("limit", "10"))
		if err != nil {
			limit = 10
		}
		fmt.Printf("q:%s, start:%s, limit:%d\n", q, start, limit)
		rt, err := index.SearchField(q, start, limit)
		c.JSON(200, gin.H{
			"value": rt,
		})
	})

	router.GET("/api/v1/field/:f/value", func(c *gin.Context) {
		f := c.Param("f")
		q := c.DefaultQuery("q", "")
		start := c.DefaultQuery("start", "")
		limit, err := strconv.Atoi(c.DefaultQuery("limit", "10"))
		if err != nil {
			limit = 10
		}
		fmt.Printf("f:%s, q:%s, start:%s, limit:%d\n", f, q, start, limit)
		rt, err := index.SearchFieldValue(f, q, start, limit)
		c.JSON(200, gin.H{
			"field": f,
			"value": rt,
		})
	})
}
