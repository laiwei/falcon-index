package http

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/laiwei/falcon-index/index"
	"strconv"
	"strings"
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

	//http://127.0.0.1:6071/api/v1/related-field/term/metric=cpu.idle
	router.GET("/api/v1/related-field/term/:term", func(c *gin.Context) {
		t := c.Param("term")
		rt, err := index.QueryFieldByTerm(t)
		if err != nil {
			c.JSON(500, gin.H{"msg": err})
		} else {
			c.JSON(200, gin.H{
				"value": rt,
			})
		}
	})

	//http://127.0.0.1:6071/api/v1/related-field/terms/metric=cpu.idle,home=bj
	router.GET("/api/v1/related-field/terms/:terms", func(c *gin.Context) {
		ts := c.Param("terms")
		terms := strings.Split(ts, ",")
		rt, err := index.QueryFieldByTerms(terms)
		if err != nil {
			c.JSON(500, gin.H{"msg": err})
		} else {
			c.JSON(200, gin.H{
				"value": rt,
			})
		}
	})

	router.GET("/api/v1/doc/term/:term", func(c *gin.Context) {
		t := c.Param("term")
		start := c.DefaultQuery("start", "")
		limit, err := strconv.Atoi(c.DefaultQuery("limit", "10"))
		if err != nil {
			limit = 10
		}
		rt, err := index.QueryDocByTerm(t, []byte(start), limit)
		if err != nil {
			c.JSON(500, gin.H{"msg": err})
		} else {
			c.JSON(200, gin.H{
				"value": rt,
			})
		}
	})

	router.GET("/api/v1/doc/terms/:terms", func(c *gin.Context) {
		ts := c.Param("terms")
		terms := strings.Split(ts, ",")

		offset_bucket := c.DefaultQuery("offset_bucket", "")
		offset_pos := c.DefaultQuery("offset_position", "")

		var offset *index.Offset
		if offset_bucket != "" && offset_pos != "" {
			offset = &index.Offset{
				Bucket:   []byte(offset_bucket),
				Position: []byte(offset_pos),
			}
		}

		limit, err := strconv.Atoi(c.DefaultQuery("limit", "10"))
		if err != nil {
			limit = 10
		}

		fmt.Printf("index.QueryDocByTerms, terms:%v, offset:%v, limit:%v\n", terms, offset, limit)
		rt, next_offset, err := index.QueryDocByTerms(terms, offset, limit)
		if err != nil {
			c.JSON(500, gin.H{"msg": err})
		} else {
			c.JSON(200, gin.H{
				"value":           rt,
				"offset_bucket":   string(next_offset.Bucket),
				"offset_position": string(next_offset.Position),
			})
		}
	})
}
