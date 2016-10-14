package main

import "testing"
import "fmt"
import "github.com/laiwei/falcon-index/g"
import "github.com/laiwei/falcon-index/index"

func init() {
	g.OpenDB()
}

func BenchmarkQueryDocByTerm(b *testing.B) {
	for i := 0; i < b.N; i++ {
		index.QueryDocByTerm("home=bj", []byte(""), 50)
	}
}

func BenchmarkQueryDocByTerms(b *testing.B) {
	for i := 0; i < b.N; i++ {
		index.QueryDocByTerms([]string{"home=bj", "metric=cpu.idle"}, nil, 5)
	}
}

func BenchmarkQueryFieldByTerm(b *testing.B) {
	for i := 0; i < b.N; i++ {
		index.QueryFieldByTerm("home=bj")
	}
}

func BenchmarkQueryFieldByTerms(b *testing.B) {
	for i := 0; i < b.N; i++ {
		index.QueryFieldByTerms([]string{"home=bj", "metric=cpu.idle"})
	}
}

func TestQueryDocByTerm(t *testing.T) {
	docs, err := index.QueryDocByTerm("home=bj", []byte(""), 2)
	for _, doc := range docs {
		fmt.Printf("%v, %v\n", doc, err)
	}
}
func TestQueryDocByTerms(t *testing.T) {
	//endpoint=laiwei-test1 5bd9dee871d734fc94aaf7ebbe40610f
	//docs, offset, err := index.QueryDocByTerms([]string{"home=bj", "endpoint=laiwei-test1"}, nil, 2)
	offset := &index.Offset{
		Bucket:   []byte("endpoint=laiwei-test1"),
		Position: []byte("5bd9dee871d734fc94aaf7ebbe40610f"),
	}
	docs, offset, err := index.QueryDocByTerms([]string{"home=bj", "endpoint=laiwei-test1"}, offset, 2)
	if err != nil {
		fmt.Printf("query by terms error:%v\n", err)
	}
	fmt.Printf("---TestQueryDocByTerms %v %v %v, %v\n", docs, string(offset.Bucket), string(offset.Position), err)
}

func TestQueryFieldByTerm(t *testing.T) {
	rt, err := index.QueryFieldByTerm("home=bj")
	fmt.Printf("%s, %v\n", rt, err)
}

func TestQueryFieldByTerms(t *testing.T) {
	rt, err := index.QueryFieldByTerms([]string{"home=bj", "metric=cpu.idle"})
	fmt.Printf("%s, %v\n", rt, err)
}

func TestSearchField(t *testing.T) {
	rt, err := index.SearchField("", "", 100)
	fmt.Printf("%s, %v\n", rt, err)
}

func TestSearchFieldValue(t *testing.T) {
	rt, err := index.SearchFieldValue("endpoint", "laiwei", "", 10)
	fmt.Printf("%s, %v\n", rt, err)
}
