# PD部署说明

## PD配置

- 配置文件在application.yml

````
license:
  # 验证使用的配置文件所在目录，包括主题、密码等
  verify-path: 'conf/verify-license.json'
  # license文件所在目录，通过hugegraph-signature项目生成
  license-path: 'conf/hugegraph.license'
pd:
  # 存储路径
  data-path: ./pd_data
  # 自动扩容的检查周期，定时检查每个Store的分区数量，自动进行分区数量平衡
  patrol-interval: 1800
  # 是否允许批量单副本入库
  enable-batch-load: false
store:
  # store下线时间。超过该时间，认为store永久不可用，分配副本到其他机器，单位秒
  max-down-time: 172800
partition:
  # 默认每个分区副本数
  default-shard-count: 3
  # 默认每机器最大副本数,初始分区数= store-max-shard-count * store-number / default-shard-count
  store-max-shard-count: 12
````

##store配置
-配置文件在application.yml,配置pdserver的address

````
pdserver:
  # pd服务地址，多个pd地址用逗号分割
  address: pdserver ip:端口
````

## Hugegraph配置

- 配置项在hugegraph的启动脚本start-hugegraph.sh中

````
if [ -z "$META_SERVERS" ];then
  META_SERVERS="pdserver ip:端口"
fi
if [ -z "$PD_PEERS" ];then
  PD_PEERS="pdserver ip:端口"
fi
````

## RESTFUL API

- pd提供了一些restful API可以获取集群分区，图，存储节点等一系列信息

###获取集群统计信息

#### 获取集群统计信息

###### Method & Url

```
GET http://localhost:8620/v1/cluster
```

###### Response Status

```json
200
```

###### Response Body

```json
{
  "message": "OK",
  "data": {
    "state": "Cluster_OK",
    "pdList": [
      {
        "raftUrl": "10.232.132.38:8610",
        "grpcUrl": "10.232.132.38:8686",
        "restUrl": "10.232.132.38:8620",
        "state": "Up",
        "dataPath": "./pd_data",
        "role": "Leader",
        "serviceName": "10.232.132.38:8686-PD",
        "serviceVersion": "",
        "startTimeStamp": 0
      }
    ],
    "pdLeader": {
      "raftUrl": "10.232.132.38:8610",
      "grpcUrl": "10.232.132.38:8686",
      "restUrl": "10.232.132.38:8620",
      "state": "Up",
      "dataPath": "./pd_data",
      "role": "Leader",
      "serviceName": "10.232.132.38:8686-PD",
      "serviceVersion": "",
      "startTimeStamp": 0
    },
    "memberSize": 1,
    "stores": [
      {
        "storeId": 110645464809417136,
        "address": "10.232.132.38:8500",
        "raftAddress": "10.232.132.38:8510",
        "version": "3.6.3",
        "state": "Up"
      }
    ],
    "storeSize": 1,
    "onlineStoreSize": 1,
    "offlineStoreSize": 0,
    "graphSize": 3,
    "partitionSize": 4,
    "shardCount": 3,
    "keyCount": 1707,
    "dataSize": 19
  },
  "status": 0
}
```

#### 获取pd集群成员信息

###### Method & Url

```
GET http://localhost:8620/v1/member
```

###### Response Status

```json
200
```

###### Response Body

```json
{
  "message": "OK",
  "data": {
    "pdLeader": {
      "raftUrl": "10.232.132.38:8610",
      "grpcUrl": "10.232.132.38:8686",
      "restUrl": "10.232.132.38:8620",
      "state": "Up",
      "dataPath": "./pd_data",
      "role": "Leader",
      "serviceName": "10.232.132.38:8686-PD",
      "serviceVersion": "",
      "startTimeStamp": 0
    },
    "pdList": [
      {
        "raftUrl": "10.232.132.38:8610",
        "grpcUrl": "10.232.132.38:8686",
        "restUrl": "10.232.132.38:8620",
        "state": "Up",
        "dataPath": "./pd_data",
        "role": "Leader",
        "serviceName": "10.232.132.38:8686-PD",
        "serviceVersion": "",
        "startTimeStamp": 0
      }
    ],
    "state": "Cluster_OK"
  },
  "status": 0
}
```

###存储节点相关

#### 获取集群所有的store的信息

###### Method & Url

```
GET http://localhost:8620/v1/stores
```

###### Response Status

```json
200
```

###### Request Body

```json
{
  "message": "OK",
  "data": {
    "stores": [
      {
        "storeId": 110645464809417136,
        "address": "10.232.132.38:8500",
        "raftAddress": "10.232.132.38:8510",
        "version": "3.6.3",
        "state": "Up",
        "deployPath": "",
        "startTimeStamp": 1658491024,
        "lastHeatBeat": 1658491748560,
        "capacity": 1968740712448,
        "available": 1959665557504,
        "partitionCount": 4,
        "graphSize": 3,
        "keyCount": 1128,
        "leaderCount": 4,
        "serviceName": "10.232.132.38:8500-store",
        "serviceVersion": "3.6.3",
        "partitions": [
          {
            "partitionId": 0,
            "graphName": "DEFAULT/hugegraph/s",
            "role": "Leader",
            "workState": "PState_Normal"
          },
          {
            "partitionId": 0,
            "graphName": "DEFAULT/hugegraph/g",
            "role": "Leader",
            "workState": "PState_Normal"
          },
          {
            "partitionId": 1,
            "graphName": "DEFAULT/hugegraph/g",
            "role": "Leader",
            "workState": "PState_Normal"
          },
          {
            "partitionId": 2,
            "graphName": "DEFAULT/hugegraph/g",
            "role": "Leader",
            "workState": "PState_Normal"
          },
          {
            "partitionId": 3,
            "graphName": "DEFAULT/hugegraph/g",
            "role": "Leader",
            "workState": "PState_Normal"
          },
          {
            "partitionId": 0,
            "graphName": "DEFAULT/hugegraph/m",
            "role": "Leader",
            "workState": "PState_Normal"
          }
        ]
      }
    ]
  },
  "status": 0
}
```

#### 获取单个store的信息

###### Method & Url

```
GET http://localhost:8620/v1/store/{storeId}
```

###### Response Status

```json
200
```

###### Request Body

```json
{
  "message": "OK",
  "data": {
    "storeId": 110645464809417136,
    "address": "10.232.132.38:8500",
    "raftAddress": "10.232.132.38:8510",
    "version": "3.6.3",
    "state": "Up",
    "deployPath": "",
    "startTimeStamp": 1658491024,
    "lastHeatBeat": 1658491838632,
    "capacity": 1968740712448,
    "available": 1959665549312,
    "partitionCount": 4,
    "graphSize": 3,
    "keyCount": 1128,
    "leaderCount": 4,
    "serviceName": "10.232.132.38:8500-store",
    "serviceVersion": "3.6.3",
    "partitions": [
      {
        "partitionId": 0,
        "graphName": "DEFAULT/hugegraph/s",
        "role": "Leader",
        "workState": "PState_Normal"
      },
      {
        "partitionId": 0,
        "graphName": "DEFAULT/hugegraph/g",
        "role": "Leader",
        "workState": "PState_Normal"
      },
      {
        "partitionId": 1,
        "graphName": "DEFAULT/hugegraph/g",
        "role": "Leader",
        "workState": "PState_Normal"
      },
      {
        "partitionId": 2,
        "graphName": "DEFAULT/hugegraph/g",
        "role": "Leader",
        "workState": "PState_Normal"
      },
      {
        "partitionId": 3,
        "graphName": "DEFAULT/hugegraph/g",
        "role": "Leader",
        "workState": "PState_Normal"
      },
      {
        "partitionId": 0,
        "graphName": "DEFAULT/hugegraph/m",
        "role": "Leader",
        "workState": "PState_Normal"
      }
    ]
  },
  "status": 0
}
```

### 分区相关

#### 获取分区信息

###### Method & Url

```
GET http://localhost:8620/v1/highLevelPartitions
```

###### Response Status

```json
200
```

###### Response Body

```json
{
  "message": "OK",
  "data": {
    "partitions": [
      {
        "partitionId": 0,
        "state": "PState_Normal",
        "leaderAddress": "10.232.132.38:8500",
        "keyCount": 0,
        "dataSize": 0,
        "shardState": "SState_Normal",
        "graphs": [
          {
            "graphName": "DEFAULT/hugegraph/g",
            "keyCount": 361,
            "startKey": 0,
            "endKey": 0,
            "dataSize": 8,
            "workState": "PState_Normal",
            "partitionId": 0
          },
          {
            "graphName": "DEFAULT/hugegraph/m",
            "keyCount": 361,
            "startKey": 0,
            "endKey": 0,
            "dataSize": 13,
            "workState": "PState_Normal",
            "partitionId": 0
          },
          {
            "graphName": "DEFAULT/hugegraph/s",
            "keyCount": 361,
            "startKey": 0,
            "endKey": 65535,
            "dataSize": 6,
            "workState": "PState_Normal",
            "partitionId": 0
          }
        ],
        "shards": [
          {
            "storeId": 110645464809417136,
            "role": "Leader",
            "state": "SState_Normal",
            "progress": 0,
            "partitionId": 0,
            "address": "10.232.132.38:8500"
          }
        ]
      },
      {
        "partitionId": 1,
        "state": "PState_Normal",
        "leaderAddress": "10.232.132.38:8500",
        "keyCount": 0,
        "dataSize": 0,
        "shardState": "SState_Normal",
        "graphs": [
          {
            "graphName": "DEFAULT/hugegraph/g",
            "keyCount": 8,
            "startKey": 16384,
            "endKey": 32768,
            "dataSize": 5,
            "workState": "PState_Normal",
            "partitionId": 1
          }
        ],
        "shards": [
          {
            "storeId": 110645464809417136,
            "role": "Leader",
            "state": "SState_Normal",
            "progress": 0,
            "partitionId": 1,
            "address": "10.232.132.38:8500"
          }
        ]
      },
      {
        "partitionId": 2,
        "state": "PState_Normal",
        "leaderAddress": "10.232.132.38:8500",
        "keyCount": 0,
        "dataSize": 0,
        "shardState": "SState_Normal",
        "graphs": [
          {
            "graphName": "DEFAULT/hugegraph/g",
            "keyCount": 18,
            "startKey": 32768,
            "endKey": 49152,
            "dataSize": 8,
            "workState": "PState_Normal",
            "partitionId": 2
          }
        ],
        "shards": [
          {
            "storeId": 110645464809417136,
            "role": "Leader",
            "state": "SState_Normal",
            "progress": 0,
            "partitionId": 2,
            "address": "10.232.132.38:8500"
          }
        ]
      },
      {
        "partitionId": 3,
        "state": "PState_Normal",
        "leaderAddress": "10.232.132.38:8500",
        "keyCount": 0,
        "dataSize": 0,
        "shardState": "SState_Normal",
        "graphs": [
          {
            "graphName": "DEFAULT/hugegraph/g",
            "keyCount": 19,
            "startKey": 49152,
            "endKey": 65536,
            "dataSize": 8,
            "workState": "PState_Normal",
            "partitionId": 3
          }
        ],
        "shards": [
          {
            "storeId": 110645464809417136,
            "role": "Leader",
            "state": "SState_Normal",
            "progress": 0,
            "partitionId": 3,
            "address": "10.232.132.38:8500"
          }
        ]
      }
    ]
  },
  "status": 0
}
```

###获取图信息

#### 获取所有的图信息

###### Method & Url

```
GET http://localhost:8620/v1/graphs
```

###### Response Status

```json
200
```

###### Response Body

```json
{
  "message": "OK",
  "data": {
    "graphs": [
      {
        "graphName": "DEFAULT/hugegraph/g",
        "partitionCount": 4,
        "state": "PState_Normal",
        "partitions": [
          {
            "partitionId": 0,
            "graphName": "DEFAULT/hugegraph/g",
            "workState": "PState_Normal",
            "startKey": 0,
            "endKey": 16384,
            "shards": [
              {
                "partitionId": 0,
                "storeId": 110645464809417136,
                "state": "SState_Normal",
                "role": "Leader",
                "progress": 0
              }
            ]
          },
          {
            "partitionId": 1,
            "graphName": "DEFAULT/hugegraph/g",
            "workState": "PState_Normal",
            "startKey": 16384,
            "endKey": 32768,
            "shards": [
              {
                "partitionId": 1,
                "storeId": 110645464809417136,
                "state": "SState_Normal",
                "role": "Leader",
                "progress": 0
              }
            ]
          },
          {
            "partitionId": 2,
            "graphName": "DEFAULT/hugegraph/g",
            "workState": "PState_Normal",
            "startKey": 32768,
            "endKey": 49152,
            "shards": [
              {
                "partitionId": 2,
                "storeId": 110645464809417136,
                "state": "SState_Normal",
                "role": "Leader",
                "progress": 0
              }
            ]
          },
          {
            "partitionId": 3,
            "graphName": "DEFAULT/hugegraph/g",
            "workState": "PState_Normal",
            "startKey": 49152,
            "endKey": 65536,
            "shards": [
              {
                "partitionId": 3,
                "storeId": 110645464809417136,
                "state": "SState_Normal",
                "role": "Leader",
                "progress": 0
              }
            ]
          }
        ],
        "dataSize": 48,
        "keyCount": 1128,
        "nodeCount": 0,
        "edgeCount": 0
      },
      {
        "graphName": "DEFAULT/hugegraph/m",
        "partitionCount": 1,
        "state": "PState_Normal",
        "partitions": [
          {
            "partitionId": 0,
            "graphName": "DEFAULT/hugegraph/m",
            "workState": "PState_Normal",
            "startKey": 0,
            "endKey": 65535,
            "shards": [
              {
                "partitionId": 0,
                "storeId": 110645464809417136,
                "state": "SState_Normal",
                "role": "Leader",
                "progress": 0
              }
            ]
          }
        ],
        "dataSize": 48,
        "keyCount": 1128,
        "nodeCount": 0,
        "edgeCount": 0
      },
      {
        "graphName": "DEFAULT/hugegraph/s",
        "partitionCount": 1,
        "state": "PState_Normal",
        "partitions": [
          {
            "partitionId": 0,
            "graphName": "DEFAULT/hugegraph/s",
            "workState": "PState_Normal",
            "startKey": 0,
            "endKey": 65535,
            "shards": [
              {
                "partitionId": 0,
                "storeId": 110645464809417136,
                "state": "SState_Normal",
                "role": "Leader",
                "progress": 0
              }
            ]
          }
        ],
        "dataSize": 48,
        "keyCount": 1128,
        "nodeCount": 0,
        "edgeCount": 0
      }
    ]
  },
  "status": 0
}
```

#### 获取单个图信息

###### Method & Url

```
GET http://localhost:8620/v1/graph/{graphName}
```

###### Response Status

```json
200
```

###### Response Body

```json
{
  "message": "OK",
  "data": {
    "graphName": "DEFAULT/hugegraph/g",
    "partitionCount": 4,
    "state": "PState_Normal",
    "partitions": [
      {
        "partitionId": 0,
        "graphName": "DEFAULT/hugegraph/g",
        "workState": "PState_Normal",
        "startKey": 0,
        "endKey": 16384,
        "shards": [
          {
            "partitionId": 0,
            "storeId": 110645464809417136,
            "state": "SState_Normal",
            "role": "Leader",
            "progress": 0
          }
        ]
      },
      {
        "partitionId": 1,
        "graphName": "DEFAULT/hugegraph/g",
        "workState": "PState_Normal",
        "startKey": 16384,
        "endKey": 32768,
        "shards": [
          {
            "partitionId": 1,
            "storeId": 110645464809417136,
            "state": "SState_Normal",
            "role": "Leader",
            "progress": 0
          }
        ]
      },
      {
        "partitionId": 2,
        "graphName": "DEFAULT/hugegraph/g",
        "workState": "PState_Normal",
        "startKey": 32768,
        "endKey": 49152,
        "shards": [
          {
            "partitionId": 2,
            "storeId": 110645464809417136,
            "state": "SState_Normal",
            "role": "Leader",
            "progress": 0
          }
        ]
      },
      {
        "partitionId": 3,
        "graphName": "DEFAULT/hugegraph/g",
        "workState": "PState_Normal",
        "startKey": 49152,
        "endKey": 65536,
        "shards": [
          {
            "partitionId": 3,
            "storeId": 110645464809417136,
            "state": "SState_Normal",
            "role": "Leader",
            "progress": 0
          }
        ]
      }
    ],
    "dataSize": 48,
    "keyCount": 1128,
    "nodeCount": 0,
    "edgeCount": 0
  },
  "status": 0
}
```

###获取shard的信息

#### 获取所有shard的信息

###### Method & Url

```
GET http://localhost:8620/v1/shards
```

###### Response Status

```json
200
```

###### Response Body

```json
{
  "message": "OK",
  "data": {
    "shards": [
      {
        "storeId": 110645464809417136,
        "partitionId": 0,
        "role": "Leader",
        "state": "SState_Normal",
        "graphName": "DEFAULT/hugegraph/g",
        "progress": 0
      },
      {
        "storeId": 110645464809417136,
        "partitionId": 1,
        "role": "Leader",
        "state": "SState_Normal",
        "graphName": "DEFAULT/hugegraph/g",
        "progress": 0
      },
      {
        "storeId": 110645464809417136,
        "partitionId": 2,
        "role": "Leader",
        "state": "SState_Normal",
        "graphName": "DEFAULT/hugegraph/g",
        "progress": 0
      },
      {
        "storeId": 110645464809417136,
        "partitionId": 3,
        "role": "Leader",
        "state": "SState_Normal",
        "graphName": "DEFAULT/hugegraph/g",
        "progress": 0
      },
      {
        "storeId": 110645464809417136,
        "partitionId": 0,
        "role": "Leader",
        "state": "SState_Normal",
        "graphName": "DEFAULT/hugegraph/m",
        "progress": 0
      },
      {
        "storeId": 110645464809417136,
        "partitionId": 0,
        "role": "Leader",
        "state": "SState_Normal",
        "graphName": "DEFAULT/hugegraph/s",
        "progress": 0
      }
    ]
  },
  "status": 0
}
```

###服务注册

#### 注册服务

###### Method & Url

```
POST http://127.0.0.1:8620/v1/registry
```

###### Request Body

```json
200
```

###### Response Status

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

appName：所属服务名  
version：所属服务版本号  
address：服务实例地址+端口  
interval：实例心跳间隔，字符串，最大9223372036854775807  
labels: 自定义标签，若服务名为'hg'即hugeserver时，需要提供key为cores的项，进行cpu核数的验证

###### Response Body

```json
{
  "errorType": "OK",
  "message": "",
  "data": null
}
```

errorType：状态码  
message：状态码为错误时的具体出错信息  
data：无返回数据

#### 服务实例获取

###### Method & Url

```
POST http://127.0.0.1:8620/v1/registryInfo
```

###### Request Body

```json
200
```

###### Response Status

```json
{
  "appName":"aaaa",
  "version":"version1",
  "labels": {
    "aaa": "aaaavalue"
  }
}
```

以下三项可全部为空，则获取所有服务节点的信息:  
-- appName：过滤所属服务名的条件   
-- version：过滤所属服务版本号的条件，此项有值，则appName不能为空  
-- labels: 过滤自定义标签的条件

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

errorType：状态码  
message：状态码为错误时的具体出错信息  
data：获取的服务节点信息  