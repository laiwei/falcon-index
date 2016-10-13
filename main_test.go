package main

import "testing"
import "fmt"
import "github.com/laiwei/falcon-index/g"
import "github.com/laiwei/falcon-index/index"

func init() {
	g.OpenDB()
}

func BenchmarkQueryByTerm(b *testing.B) {
	for i := 0; i < b.N; i++ {
		index.QueryByTerm("home=bj", []byte(""), 50)
	}
}

func BenchmarkQueryByTerms(b *testing.B) {
	for i := 0; i < b.N; i++ {
		index.QueryByTerms([]string{"home=bj", "metric=cpu.idle"}, nil, 5)
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

func TestQueryByTerm(t *testing.T) {
	docs, err := index.QueryByTerm("home=bj", []byte(""), 2)
	for _, doc := range docs {
		fmt.Printf("%v, %v\n", doc, err)
	}
}
func TestQueryByTerms(t *testing.T) {
	docs, offset, err := index.QueryByTerms([]string{"home=bj"}, nil, 2)
	if err != nil {
		fmt.Printf("query by terms error:%v\n", err)
	}
	for _, doc := range docs {
		fmt.Printf("---%v %v %v, %v\n", doc, string(offset.Bucket), string(offset.Position), err)
	}
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
