#!/env/bin/python

import sys
import json

def gen(count):
    print "["
    for x in range(0, count):
        d = {
            "endpoint": "laiwei-test%s" %(x,),
            "metric": "cpu.idle",
            "value": 1,
            "step": 30,
            "counterType": "GAUGE",
            "tags": "home=bj,srv=falcon",
            "timestamp": 1234567
        }
        print json.dumps(d)
        print ","
    print "]"


if __name__ == "__main__":
    gen(int(sys.argv[1]))
