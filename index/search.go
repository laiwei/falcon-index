package index

import (
	"bytes"
	"fmt"
	"github.com/boltdb/bolt"
	"github.com/laiwei/falcon-index/doc"
	"github.com/laiwei/falcon-index/g"
	"log"
	"strings"
)

type Offset struct {
	Bucket   []byte
	Position []byte
}

func termsToDict(terms []string) map[string]string {
	rt := make(map[string]string)
	for _, t := range terms {
		fields := strings.SplitN(t, "=", 2)
		rt[fields[0]] = rt[fields[1]]
	}
	return rt
}

func findShortestTermDocBucket(terms []string) (string, error) {
	if len(terms) == 0 {
		return "", fmt.Errorf("empty_terms")
	}

	if len(terms) == 1 {
		return terms[0], nil
	}

	terms_dict := make(map[string]string)
	for _, t := range terms {
		terms_dict[t] = ""
	}

	metric_t := ""
	for _, t := range terms {
		if strings.HasPrefix(t, "metric=") {
			metric_t = t
			break
		}
	}

	terms_ := make([]string, 0)
	if metric_t != "" {
		delete(terms_dict, metric_t)
		for t, _ := range terms_dict {
			terms_ = append(terms_, metric_t+","+t)
		}
	} else {
		terms_ = terms
	}

	rt_bucket := ""
	var min_sz int64 = -1

	err := g.KVDB.View(func(tx *bolt.Tx) error {
		for _, term := range terms_ {
			term_bname := g.TERM_DOCS_BUCKET_PREFIX + term
			b := tx.Bucket([]byte(term_bname))
			if b == nil {
				return fmt.Errorf("no_such_bucket:%s", term)
			}
			sb := tx.Bucket([]byte(g.SIZE_BUCKET))
			if sb == nil {
				return fmt.Errorf("no_such_bucket_size:%s", term)
			}
			sz := sb.Get([]byte(term))
			if sz == nil || len(sz) == 0 {
				return fmt.Errorf("empty_bucket:%s", term)
			}
			isz := g.BytesToInt64(sz)
			if min_sz < 0 || isz <= min_sz {
				min_sz = isz
				rt_bucket = term
			}
		}
		return nil
	})

	if err != nil {
		return "", err
	}
	return rt_bucket, nil
}

func QueryDocByTerm(term string, start []byte, limit int) ([]*doc.Doc, error) {
	docs := make([]*doc.Doc, 0)

	err := g.KVDB.View(func(tx *bolt.Tx) error {
		i := 0

		td_bucket_key := g.TERM_DOCS_BUCKET_PREFIX + term
		b := tx.Bucket([]byte(td_bucket_key))
		if b == nil {
			return fmt.Errorf("non-exists-bucket:%s", td_bucket_key)
		}
		c := b.Cursor()

		var k, v []byte
		if start == nil || len(start) == 0 {
			k, v = c.First()
		} else {
			c.Seek(start)
			k, v = c.Next()
		}
		for ; i < limit && k != nil; k, v = c.Next() {
			mdoc := &doc.MetaDoc{}
			err := mdoc.Unmarshal(v)
			//NOTICE:
			if err != nil {
				log.Printf("decode doc:%s fail:%s", v, err)
				continue
			}
			i++
			doc_ := &doc.Doc{
				ID:      string(k),
				MetaDoc: mdoc,
			}
			docs = append(docs, doc_)
		}
		return nil
	})

	if err != nil {
		log.Printf("search term_bucket fail:%s", err)
		return []*doc.Doc{}, err
	}

	return docs, nil
}

func QueryDocByTerms(terms []string, start *Offset, limit int) ([]*doc.Doc, *Offset, error) {
	var rt_offset *Offset
	var sterm string
	var start_pos []byte

	var err error
	docs := make([]*doc.Doc, 0)
	if start == nil {
		sterm, err = findShortestTermDocBucket(terms)
		if err != nil {
			return docs, nil, err
		}
		start_pos = nil
	} else {
		sterm = string(start.Bucket)
		start_pos = start.Position
	}

	n := 0
	terms_dict := termsToDict(terms)
	for {
		candidate_docs, err := QueryDocByTerm(sterm, start_pos, limit*2)
		if err != nil {
			return []*doc.Doc{}, nil, err
		}

		if len(candidate_docs) == 0 {
			break
		}

		for _, d := range candidate_docs {
			start_pos = []byte(d.ID)
			if n >= limit {
				goto END
			}

			hit := false
			d_dict := d.TermDict()
			for k, v := range terms_dict {
				if v2, ok := d_dict[k]; ok && v2 == v {
				} else {
					hit = true
					break
				}
			}
			if hit {
				n = n + 1
				docs = append(docs, d)
			}
		}
	}

END:
	if !bytes.Equal(start_pos, nil) {
		rt_offset = &Offset{
			Bucket: []byte(sterm), Position: start_pos,
		}
	} else {
		rt_offset = nil
	}

	return docs, rt_offset, nil
}

func QueryFieldByTerm(term string) ([]string, error) {
	rt := make([]string, 0)

	err := g.KVDB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(g.TERM_FIELDS_BUCKET))
		if b == nil {
			return fmt.Errorf("non-exists-bucket:%s", g.TERM_FIELDS_BUCKET)
		}

		var buf bytes.Buffer
		var k []byte
		var prefix []byte

		c := b.Cursor()
		buf.WriteString(term)
		buf.WriteByte(30)
		prefix = buf.Bytes()
		k, _ = c.Seek(prefix)

		for ; k != nil && bytes.HasPrefix(k, prefix); k, _ = c.Next() {
			rt = append(rt, string(bytes.TrimPrefix(k, prefix)))
		}
		return nil
	})

	if err != nil {
		return []string{}, err
	}

	return rt, nil
}

func QueryFieldByTerms(terms []string) ([]string, error) {
	rt := make([]string, 0)

	for _, t := range terms {
		fields, err := QueryFieldByTerm(t)
		if err != nil {
			return []string{}, err
		}

		if len(fields) == 0 {
			return []string{}, nil
		}

		if len(rt) == 0 {
			rt = fields
		} else {
			rt = g.StringSliceIntersect(rt, fields)
		}
	}

	return rt, nil
}

func SearchField(q, start string, limit int) ([]string, error) {
	rt := make([]string, 0)

	err := g.KVDB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(g.FIELDS_BUCKET))
		if b == nil {
			return fmt.Errorf("non-exists-bucket:%s", g.FIELDS_BUCKET)
		}

		var buf bytes.Buffer
		buf.WriteString(q)
		prefix := buf.Bytes()

		i := 0
		c := b.Cursor()
		var k []byte
		if start == "" {
			k, _ = c.Seek(prefix)
		} else {
			c.Seek([]byte(start))
			k, _ = c.Next()
		}

		for ; i < limit && k != nil && bytes.HasPrefix(k, prefix); k, _ = c.Next() {
			i++
			rt = append(rt, string(k))
		}
		return nil
	})

	if err != nil {
		return []string{}, err
	}

	return rt, nil
}

func SearchFieldValue(f, q, start string, limit int) ([]string, error) {
	rt := make([]string, 0)

	err := g.KVDB.View(func(tx *bolt.Tx) error {
		bk := g.FVALUE_BUCKET_PREFIX + f
		b := tx.Bucket([]byte(bk))
		if b == nil {
			return fmt.Errorf("non-exists-bucket:%s", bk)
		}

		var buf bytes.Buffer
		buf.WriteString(q)
		prefix := buf.Bytes()

		i := 0
		c := b.Cursor()
		var k []byte
		if start == "" {
			k, _ = c.Seek(prefix)
		} else {
			c.Seek([]byte(start))
			k, _ = c.Next()
		}

		for ; i < limit && k != nil && bytes.HasPrefix(k, prefix); k, _ = c.Next() {
			i++
			rt = append(rt, string(k))
		}
		return nil
	})

	if err != nil {
		return []string{}, err
	}

	return rt, nil
}

func QueryFieldValueByTerms(terms []string, start *Offset, limit int, f, q string) ([]string, *Offset, error) {
	rt := make([]string, 0)
	if f == "" {
		return rt, nil, fmt.Errorf("no_field")
	}
	if len(terms) == 0 {
		return rt, nil, fmt.Errorf("empty_terms")
	}

	var rt_offset *Offset
	for {
		docs, new_offset, err := QueryDocByTerms(terms, start, limit*2)
		if err != nil {
			return rt, nil, err
		}
		start = new_offset

		if len(docs) == 0 {
			break
		}
		for _, d := range docs {
			rt_offset = &Offset{
				Bucket:   new_offset.Bucket,
				Position: []byte(d.ID),
			}

			if len(rt) >= limit {
				goto END
			}

			f_dict := d.TermDict()
			if fv, ok := f_dict[f]; ok && strings.Contains(fv, q) {
				rt = append(rt, fv)
			}
		}
	}
END:
	return rt, rt_offset, nil
}
