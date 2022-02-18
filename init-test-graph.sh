#!/bin/bash

#mvn package
mvn clean package -DskipTests
#clear etcd
etcdctl del --prefix HUGEGRAPH/ 
#start server
cd hugegraph-3.0.0/ && ./bin/start-hugegraph.sh 

#init basic data

# graph space
curl --location --request POST 'http://localhost:8080/graphspaces' \
--header 'Authorization: Basic YWRtaW46YWRtaW4=' \
--header 'Content-Type: application/json' \
--data-raw '{
    "name": "gs1",
    "description": "Test graphSpace 1",
    "cpu_limit": 1,
    "memory_limit": 4,
    "storage_limit": 16,
    "oltp_namespace": "null",
    "olap_namespace": "null",
    "storage_namespace": "null",
    "max_graph_number": 16,
    "max_role_number": 16,
    "auth": true,
    "configs": {}
}'

#graph 

curl --location --request POST 'http://localhost:8080/graphspaces/gs1/graphs/hg1' \
--header 'Authorization: Basic YWRtaW46YWRtaW4=' \
--header 'Content-Type: application/json' \
--data-raw '{
    "gremlin.graph": "com.baidu.hugegraph.HugeFactory",
    "backend": "memory",
    "serializer": "text",
    "store": "hg1",
    "search.text_analyzer": "jieba",
    "search.text_analyzer_mode": "INDEX",
    "task.scheduler_type": "etcd"
}'

# properties

curl --location --request POST 'http://localhost:8080/graphspaces/gs1/graphs/hg1/schema/propertykeys' \
--header 'Authorization: Basic YWRtaW46YWRtaW4=' \
--header 'Content-Type: application/json' \
--data-raw '


{
    "name": "age",
    "data_type": "INT",
    "cardinality": "SINGLE"
}
'

curl --location --request POST 'http://localhost:8080/graphspaces/gs1/graphs/hg1/schema/propertykeys' \
--header 'Authorization: Basic YWRtaW46YWRtaW4=' \
--header 'Content-Type: application/json' \
--data-raw '


{
    "name": "name",
    "data_type": "TEXT",
    "cardinality": "SINGLE"
}

'

curl --location --request POST 'http://localhost:8080/graphspaces/gs1/graphs/hg1/schema/propertykeys' \
--header 'Authorization: Basic YWRtaW46YWRtaW4=' \
--header 'Content-Type: application/json' \
--data-raw '

{
    "name": "weight",
    "data_type": "DOUBLE",
    "cardinality": "SINGLE"
}

'

curl --location --request POST 'http://localhost:8080/graphspaces/gs1/graphs/hg1/schema/propertykeys' \
--header 'Authorization: Basic YWRtaW46YWRtaW4=' \
--header 'Content-Type: application/json' \
--data-raw '

{
    "name": "date",
    "data_type": "DATE",
    "cardinality": "SINGLE"
}
'

### labels
curl --location --request POST 'http://localhost:8080/graphspaces/gs1/graphs/hg1/schema/vertexlabels' \
--header 'Authorization: Basic YWRtaW46YWRtaW4=' \
--header 'Content-Type: application/json' \
--data-raw '{
    "name": "person",
    "id_strategy": "DEFAULT",
    "properties": [
        "name",
        "age"
    ],
    "primary_keys": [
        "name"
    ],
    "nullable_keys": [],
    "ttl": 86400000,
    "enable_label_index": true
}'

curl --location --request POST 'http://localhost:8080/graphspaces/gs1/graphs/hg1/schema/edgelabels' \
--header 'Authorization: Basic YWRtaW46YWRtaW4=' \
--header 'Content-Type: application/json' \
--data-raw '{
    "name": "relation",
    "source_label": "person",
    "target_label": "person",
    "frequency": "SINGLE",
    "properties": [
        "date",
        "weight"
    ],
    "sort_keys": [],
    "nullable_keys": [],
    "enable_label_index": true
}'

#vertex

curl --location --request POST 'http://localhost:8080/graphspaces/gs1/graphs/hg1/graph/vertices/batch' \
--header 'Authorization: Basic YWRtaW46YWRtaW4=' \
--header 'Content-Type: application/json' \
--data-raw '[
    {
        "label": "person",
        "properties": {
            "name": "marko",
            "age": 29
        }
    },
    {
        "label": "person",
        "properties": {
            "name": "ripple",
            "age": 27
        }
    }
]'

#edge

curl --location --request POST 'http://localhost:8080/graphspaces/gs1/graphs/hg1/graph/edges' \
--header 'Authorization: Basic YWRtaW46YWRtaW4=' \
--header 'Content-Type: application/json' \
--data-raw '{
    "label": "relation",
    "outV":  "1:marko",
    "inV": "1:ripple",
    "outVLabel": "person",
    "inVLabel": "person",
    "properties": {
        "date": "2021-5-20",
        "weight": 0.2
    }
}'

