package g

import (
	"log"
	"runtime"
)

const (
	VERSION                 = "0.1"
	SIZE_BUCKET             = "_sz_"
	FIELDS_BUCKET           = "_f_"
	FVALUE_BUCKET_PREFIX    = "_v_"
	TERM_DOCS_BUCKET_PREFIX = "_t_"
	TERM_FIELDS_BUCKET      = "_tf_"
)

func init() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
}
