# HStore存储部署说明

##安装etcd

- 配置etcd时需要确保其他服务器可以访问，注意配置文件中以下修改

````
  name: etcd-1 #节点名称
  data-dir: default.etcd/ #节点数据存储目录
  listen-client-urls: http://0.0.0.0:2379  #监听etcd客户发送信息的地址
  advertise-client-urls: http://0.0.0.0:2379  #客户与本节点交互信息所用地址
````

## PD配置

- 配置文件在application.yml

````
  grpc:
    port: 9000 #内部grpc通信用端口，如心跳等使用
    max-inbound-message-size: 100MB #grpc最大入参的大小
  server:
    port : 9001 #restful端口，如通过http的get方法获取storeNode信息(/v1/stores)等
  etcd:
    address: http://localhost:2379 #etcd地址
    timeout: 3  #etcd连接超时时间
  store:    
    keepAlive-timeout: 60 #store心跳超时时间，超过该时间，认为store临时不可用，转移Leader到其他副本,单位秒
    down-time: 1800 #store下线时间。超过该时间，认为store永久不可用，分配副本到其他机器，单位秒
  partition:    
    default-total-count: 12 #默认分区总数
    default-shard-count: 3 #默认每个分区副本数
````

## StoreNode配置

```` 
  grpc:
    port: 9080    
    host: 127.0.0.1 #grpc的服务地址，给HugeServer使用
    netty-server:
      max-inbound-message-size: 100MB #grpc最大入参的大小
  server:
    port : 9090 #restful端口，如通过http的get方法获取监控统计信息(/metrics)等
  app:
    rocks:
      config-path: "/hugegraph.properties" #写入Rocksdb的参数配置文件
  raft:
    address: 127.0.0.1:8081 #raft通信地址
    data-path: /test/raft/8081 #raft日志写入目录
    timeout: 10 #raft读写超时时间,单位秒
    metrics: false #统计raft监控统计信息，打开可能对性能有影响
  pdserver:
    address: localhost:9000 #PD的grpc地址
````

StoreNode在启动后，如果在PD的日志中打印出其注册信息，则代表注册成功，如：Store register, id = ****
address = ****

## Hugegraph配置

- 配置文件在properties文件，如hugegraph.properties

````
  backend=hstore #后端分布式存储类型，固定为hstore
  serializer=binary #序列化器，固定为binary
  pd.peers=localhost:9000 #PD的grpc地址
````

## RESTFUL API

- pd提供了一些restful API可以获取分区、存储节点等一系列信息

###图相关

#### 获取图的partition、shard信息

###### Method & Url

```
GET http://localhost:9001/v1/graphs
```

###### Response Status

```json
200
```

###### Response Body

```json
{
  "status": 0,
  "graphs": [
    {
      "graphName": "default/hugegraph/g",
      "partitionCount": 60,
      "shardCount": 3
    },
    {
      "graphName": "default/hugegraph/m",
      "partitionCount": 60,
      "shardCount": 3
    },
    {
      "graphName": "default/hugegraph/s",
      "partitionCount": 60,
      "shardCount": 3
    },
    {
      "graphName": "default/system/g",
      "partitionCount": 60,
      "shardCount": 3
    },
    {
      "graphName": "default/system/m",
      "partitionCount": 60,
      "shardCount": 3
    },
    {
      "graphName": "default/system/s",
      "partitionCount": 60,
      "shardCount": 3
    }
  ]
}
```

#### 获取指定图的partition、shard信息

###### Method & Url

```
GET http://localhost:9001/v1/graph/default/hugegraph/g
```

###### Response Status

```json
200
```

###### Response Body

```json
{
  "status": 0,
  "graph": {
    "graphName": "default/hugegraph/g",
    "partitionCount": 60,
    "shardCount": 3
  }
}
```

#### 修改指定图的partition、shard信息

###### Method & Url

```
POST http://localhost:9001/v1/graph/default/hugegraph/g
```

###### Request Body

```json
{
  "partitionCount":90,
  "shardCount": 4,
  "workMode":"Batch_Import"
}
```

###### Response Status

```json
200
```

###### Response Body

```json
{
  "status": 0,
  "graph": {
    "graphName": "default/hugegraph/g",
    "partitionCount": 60,
    "shardCount": 4,
    "workMode": "Batch_Import"
  }
}
```

### 分区相关

#### 获取分区信息

###### Method & Url

```
GET http://localhost:9001/v1/partitions
```

###### Response Status

```json
200
```

###### Response Body

```json
{
  "partitions": [
    { 
      "endKey": 2731,
      "graphName": "DEFAULT/hugegraph18e1/g",
      "id": 0,
      "shards": [
        {
          "address": "10.14.139.56:8500",
          "committedIndex": "7396553",
          "role": "Follower",
          "state": "online",
          "storeId": "6140115507143799097"
        },
        {
          "address": "10.14.139.61:8500",
          "committedIndex": "7399964",
          "role": "Follower",
          "state": "online",
          "storeId": "3661113370728329034"
        },
        {
          "address": "10.14.139.57:8500",
          "committedIndex": "7394615",
          "role": "Leader",
          "state": "online",
          "storeId": "3384290400781292563"
        }
      ],
      "startKey": 0,
      "timestamp": "2022-03-28 17:29:56",
      "version": 23,
      "workMode": "Normal",
      "workState": "State_Normal"
    }
  ],
  "status": 0
}
```

###存储节点相关

#### 获取存储节点信息

###### Method & Url

```
GET http://localhost:9001/v1/stores
```

###### Response Status

```json
200
```

###### Response Body

```json
{
  "status": 0,
  "stores": [
    {
      "address": "10.14.139.60:8500",
      "dataVersion": 1,
      "id": "1779882393067551022",
      "labels": [
        {
          "key": "rest.port",
          "value": "8520"
        }
      ],
      "lastHeartbeat": "1648459470389",
      "raftAddress": "10.14.139.60:8510",
      "startTimestamp": "1648395903516",
      "state": "Up",
      "stats": {
        "available": "4081010491392",
        "capacity": "7545950388224",
        "graphStats": [
          {
            "approximateKeys": "1933169027",
            "approximateSize": "191628815980",
            "committedIndex": "7301460",
            "graphName": "DEFAULT/hugegraph18e1/g",
            "partitionId": 1,
            "role": "Leader",
            "workMode": "Normal",
            "workState": "State_Normal"
          },
          {
            "approximateKeys": "1919472248",
            "approximateSize": "189192672333",
            "committedIndex": "7301705",
            "graphName": "DEFAULT/hugegraph18e1/g",
            "partitionId": 2,
            "role": "Leader",
            "workMode": "Normal",
            "workState": "State_Normal"
          }
        ],
        "partitionCount": 12,
        "startTime": 1648395902,
        "storeId": "1779882393067551022",
        "usedSize": "4851786575920"
      },
      "version": "3.6.3"
    }
  ]
}
```

#### 删除存储节点

###### Method & Url

```
DELETE http://localhost:9001/v1/store/2549332866463202176
```

###### Response Status

```json
200
```

###### Response Body

````
OK
````

#### 服务注册

###### Method & Url

```
post http://127.0.0.1:8620/v1/registry
```

###### Request Body

```json
{
  "appName":"aaaa",
  "version":"version1",
  "address":"address1",
  "interval":"9223372036854775807",
  "labels": {
    "aaa": "aaaavalue"
  }
}
```

```` 
appName：所属服务名
version：所属服务版本号
address：服务实例地址+端口
interval：实例心跳间隔，字符串，最大9223372036854775807
labels: 自定义标签
```` 

###### Response Status

```json
200
```

###### Response Body

```json
{
	"errorType": "OK",
	"message": "",
	"data": null
}
````

```` 
errorType：状态码
message：状态码为错误时的具体出错信息
data：无返回数据
```` 

#### 服务实例获取

###### Method & Url

```
post http://127.0.0.1:8620/v1/registryInfo
```

###### Request Body

```json
{
  "appName":"aaaa",
  "version":"version1",
  "labels": {
    "aaa": "aaaavalue"
  }
}
```

```` 
以下三项可全部为空，则获取所有服务节点的信息
    appName：过滤所属服务名的条件
    version：过滤所属服务版本号的条件，此项有值，则appName不能为空
    labels: 过滤自定义标签的条件
```` 

###### Response Status

```json
200
```

###### Response Body

```json
{
    "errorType": "OK",
    "message": null,
    "data": [
        {
            "id": null,
            "appName": "aaaa",
            "version": "version1",
            "address": "address1",
            "interval": "9223372036854775807",
            "labels": {
                "aaa": "aaaavalue"
            }
        }
    ]
}
```

```` 
errorType：状态码
message：状态码为错误时的具体出错信息
data：获取的服务节点信息
```` 

#### 服务注册

###### Method & Url

```
post http://127.0.0.1:8620/v1/registry
```

###### Request Body

```json
{
  "appName":"aaaa",
  "version":"version1",
  "address":"address1",
  "interval":"9223372036854775807",
  "labels": {
    "aaa": "aaaavalue"
  }
}
```

```` 
appName：所属服务名
version：所属服务版本号
address：服务实例地址+端口
interval：实例心跳间隔，字符串，最大9223372036854775807
labels: 自定义标签
```` 

###### Response Status

```json
200
```

###### Response Body

```json
{
	"errorType": "OK",
	"message": "",
	"data": null
}
````

```` 
errorType：状态码
message：状态码为错误时的具体出错信息
data：无返回数据
```` 

#### 获取pd成员

###### Method & Url

```
GET http://127.0.0.1:8620/v1/members
```

###### Response Status

```json
200
```

###### Response Body

```json
{
  "status": 0,
  "graphs": {
    "members": [
      {
        "clusterId": "1",
        "raftUrl": "10.14.139.56:8620",
        "grpcUrl": "10.14.139.56:8686",
        "restUrl": "10.14.139.56:8620",
        "dataPath": "./pd_data",
        "state": "Up"
      },
      {
        "clusterId": "1",
        "raftUrl": "10.14.139.57:8620",
        "grpcUrl": "10.14.139.57:8686",
        "restUrl": "10.14.139.57:8620",
        "dataPath": "./pd_data",
        "state": "Up"
      },
      {
        "clusterId": "1",
        "raftUrl": "10.14.139.58:8620",
        "grpcUrl": "10.14.139.58:8686",
        "restUrl": "10.14.139.58:8620",
        "dataPath": "./pd_data",
        "state": "Up"
      }
    ],
    "leader": {
      "clusterId": "1",
      "raftUrl": "10.14.139.56:8610",
      "grpcUrl": "10.14.139.56:8686",
      "dataPath": "./pd_data",
      "state": "Up"
    }
  }
}
```

```` 
clusterId：pd集群ID
raftUrl、grpcUrl、restUrl：pd节点的地址信息
dataPath：pd数据存储目录
state: 节点当前状态
```` 

#### Store Mertics 地址

```
GET http://127.0.0.1:8520/actuator/prometheus

```