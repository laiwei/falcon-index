package index

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/boltdb/bolt"
	"github.com/golang/protobuf/proto"
	"github.com/laiwei/falcon-index/doc"
	"github.com/laiwei/falcon-index/g"
	cmodel "github.com/open-falcon/common/model"
	cutils "github.com/open-falcon/common/utils"
	"github.com/toolkits/file"
	"log"
)

// each term as a bucket, for seek speedup, and save doc together
func BuildIndex() {
	sz_bname := []byte(g.SIZE_BUCKET)
	field_bname := []byte(g.FIELDS_BUCKET)
	tf_bname := []byte(g.TERM_FIELDS_BUCKET)
	g.KVDB.Update(func(tx *bolt.Tx) error {
		tx.CreateBucketIfNotExists(sz_bname)
		tx.CreateBucketIfNotExists(field_bname)
		tx.CreateBucketIfNotExists(tf_bname)
		return nil
	})

	testContent, err := file.ToTrimString("./test-metadata.json")
	if err != nil {
		log.Fatalln("read test data file error:", err.Error())
	}

	var data []*cmodel.JsonMetaData
	err = json.Unmarshal([]byte(testContent), &data)
	if err != nil {
		log.Fatalln("parse test file error:", err.Error())
	}

	for _, jmd := range data {
		d := &doc.MetaDoc{
			Endpoint:    proto.String(jmd.Endpoint),
			Metric:      proto.String(jmd.Metric),
			CounterType: proto.String(jmd.CounterType),
			Step:        proto.Int64(jmd.Step),
			Tags:        []*doc.Pair{},
		}
		tags := cutils.DictedTagstring(jmd.Tags)
		for tagk, tagv := range tags {
			p := &doc.Pair{
				Key:   proto.String(tagk),
				Value: proto.String(tagv),
			}
			d.Tags = append(d.Tags, p)
		}
		log.Printf("doc:%v\n", d)

		doc_id := cutils.Checksum(d.GetEndpoint(), d.GetMetric(), tags) // ps: counter的随机分布,当设置limit时 相近的counter不能在同一批返回
		doc_bytes, err := d.Marshal()
		if err != nil {
			log.Fatalln("marshal doc:%s", err)
		}

		g.KVDB.Update(func(tx *bolt.Tx) error {
			tags_ := tags
			tags_["metric"] = d.GetMetric()
			tags_["endpoint"] = d.GetEndpoint()

			sz_bucket := tx.Bucket(sz_bname)
			tf_bucket := tx.Bucket(tf_bname)
			var buf bytes.Buffer
			for k, v := range tags_ {
				buf.Reset()
				buf.WriteString(k)
				buf.WriteString("=")
				buf.WriteString(v)
				term := buf.Bytes()

				//term_bucket, _ => [doc_id:doc, doc_id:doc...]
				term_bname := append([]byte(g.TERM_DOCS_BUCKET_PREFIX), term...)
				term_bucket, err := tx.CreateBucketIfNotExists(term_bname)
				if err != nil {
					return fmt.Errorf("create-term-bucket:%s", err)
				}
				term_bucket.Put([]byte(doc_id), doc_bytes)

				//size_bucket, _ => [term1:size, term2:size]
				sz := sz_bucket.Get(term)
				if sz == nil || len(sz) == 0 {
					sz_bucket.Put(term, g.Int64ToBytes(1))
				} else {
					new_sz := g.BytesToInt64(sz) + 1
					sz_bucket.Put(term, g.Int64ToBytes(new_sz))
				}

				//fields_bucket,  _ => [field1:"", field2:"", field3:""]
				f_bucket := tx.Bucket(field_bname)
				f_bucket.Put([]byte(k), []byte(""))

				//field_value_bucket, field => [value1:"", value2:"", value3:""]
				fv_bname := g.FVALUE_BUCKET_PREFIX + k
				fv_bucket, _ := tx.CreateBucketIfNotExists([]byte(fv_bname))
				fv_bucket.Put([]byte(v), []byte(""))

				//term_fileds, _ => [term0x00field, term0x00field, ]
				for f, _ := range tags {
					buf.Reset()
					buf.Write(term)
					buf.WriteByte(30)
					buf.WriteString(f)
					tf_bucket.Put(buf.Bytes(), []byte(""))
				}
			}

			//secondary index with metric, used for query docs by terms
			metric_v := tags_["metric"]
			delete(tags, "metric")
			for k, v := range tags_ {
				buf.Reset()
				buf.WriteString("metric=")
				buf.WriteString(metric_v)
				buf.WriteString(",")
				buf.WriteString(k)
				buf.WriteString("=")
				buf.WriteString(v)
				term := buf.Bytes()

				//term_bucket, _ => [doc_id:doc, doc_id:doc...]
				term_bname := append([]byte(g.TERM_DOCS_BUCKET_PREFIX), term...)
				term_bucket, err := tx.CreateBucketIfNotExists(term_bname)
				if err != nil {
					return fmt.Errorf("create-term-bucket:%s", err)
				}
				term_bucket.Put([]byte(doc_id), doc_bytes)

				//size_bucket, _ => [term1:size, term2:size]
				sz := sz_bucket.Get(term)
				if sz == nil || len(sz) == 0 {
					sz_bucket.Put(term, g.Int64ToBytes(1))
				} else {
					new_sz := g.BytesToInt64(sz) + 1
					sz_bucket.Put(term, g.Int64ToBytes(new_sz))
				}
			}

			return nil
		})
	}
}
