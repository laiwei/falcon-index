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
// use most disk
func BuildIndex() {
	sz_key := []byte(g.SIZE_BUCKET)
	f_key := []byte(g.FIELDS_BUCKET)
	tf_key := []byte(g.TERM_FIELDS_BUCKET)
	g.KVDB.Update(func(tx *bolt.Tx) error {
		tx.CreateBucketIfNotExists(sz_key)
		tx.CreateBucketIfNotExists(f_key)
		tx.CreateBucketIfNotExists(tf_key)
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

		doc_id := cutils.Checksum(d.GetEndpoint(), d.GetMetric(), tags)
		doc_bytes, err := d.Marshal()
		if err != nil {
			log.Fatalln("marshal doc:%s", err)
		}

		g.KVDB.Update(func(tx *bolt.Tx) error {
			tags_ := tags
			tags_["metric"] = d.GetMetric()
			tags_["endpoint"] = d.GetEndpoint()

			sz_bucket := tx.Bucket(sz_key)
			tf_bucket := tx.Bucket(tf_key)
			var buf bytes.Buffer
			for k, v := range tags_ {
				buf.Reset()
				buf.WriteString(k)
				buf.WriteString("=")
				buf.WriteString(v)
				term := buf.Bytes()

				//term_doc
				term_key := append([]byte(g.TERM_DOCS_BUCKET_PREFIX), term...)
				term_bucket, err := tx.CreateBucketIfNotExists(term_key)
				if err != nil {
					return fmt.Errorf("create term bucket: %s", err)
				}
				term_bucket.Put([]byte(doc_id), doc_bytes)

				//size
				sz := sz_bucket.Get(term)
				if sz == nil || len(sz) == 0 {
					sz_bucket.Put(term, g.Int64ToBytes(1))
				} else {
					new_sz := g.BytesToInt64(sz) + 1
					sz_bucket.Put(term, g.Int64ToBytes(new_sz))
				}

				//fields
				f_bucket := tx.Bucket(f_key)
				f_bucket.Put([]byte(k), []byte(""))

				//field_value
				fv_bucket_name := g.FVALUE_BUCKET_PREFIX + k
				fv_bucket, _ := tx.CreateBucketIfNotExists([]byte(fv_bucket_name))
				log.Printf("===put to %s, k:%s\n", fv_bucket_name, v)
				fv_bucket.Put([]byte(v), []byte(""))

				//term_fileds
				for f, _ := range tags {
					buf.Reset()
					buf.Write(term)
					buf.WriteByte(30)
					buf.WriteString(f)
					log.Printf("===put to %s, k:%s\n", tf_key, buf.Bytes())
					tf_bucket.Put(buf.Bytes(), []byte(f))
				}
			}

			return nil
		})
	}
}
