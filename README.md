- 编译

```
go get ./...
./control build
```

- 生成1000个counter作为测试数据

```
python gen_test_data.py 1000 > test-metadata.json 
```

- 建立索引文件

```
 mkdir -p ./var/ && ./falcon-index  &> buildindex.log
 ```

 建立好的索引文件就是 ./var/index.db


 - 本地测试一下

 ```
 go test
 ```


 - 跑一下benchmark

 ```
 go test -bench="."
 ```

 - 作为server端启动，提供http查询API

 ```
 ./falcon-index -s 
 ```
