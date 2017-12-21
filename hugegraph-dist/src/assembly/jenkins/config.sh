#!/bin/bash

BASE_DIR="hugegraph-test/src/main/resources"
ORIGIN_CONF="$BASE_DIR/hugegraph.properties"

STATUS=0

function config_common() {
    BACKEND=$1
    SERIALIZER=$2
    STORE=$3

    CONF="$BASE_DIR/$BACKEND.properties"

    # Copy a new config file
    cp $ORIGIN_CONF $CONF
    if [ $? -ne 0 ] ; then
        echo "Failed to copy config file: $CONF"
        exit 1
    fi

    # Set option: backend, serializer, store
    sed -i "s/backend=.*/backend=$BACKEND/" $CONF
    sed -i "s/serializer=.*/serializer=$SERIALIZER/" $CONF
    sed -i "s/store=.*/store=$STORE/" $CONF

    # Specify filter file according trigger value then set to config
    FILTER=""
    if [ $TRIGGER = "before-merge" ]; then
        FILTER="fast-methods.filter"
    elif [ $TRIGGER = "after-merge" ]; then
        FILTER="methods.filter"
    else
        echo "TRIGGER can only be 'before-merge' or 'after-merge', but got $TRIGGER"
        exit 1
    fi
    sed -i "s/test.tinkerpop.filter=.*/test.tinkerpop.filter=$FILTER/" $CONF

    echo $CONF
}

function config_memory() {

    BACKEND="memory"
    SERIALIZER="text"
    STORE="hugegraph_$BUILD_ID"

    CONF=`config_common $BACKEND $SERIALIZER $STORE`
    if [ $? -ne 0 ]; then
        echo $CONF
        exit 1
    fi

    echo $CONF
}

function config_cassandra() {

    BACKEND="cassandra"
    SERIALIZER="cassandra"
    STORE="hugegraph_$BUILD_ID"
    HOST=${CASSANDRA_HOST}
    PORT=${CASSANDRA_PORT}

    CONF=`config_common $BACKEND $SERIALIZER $STORE`
    if [ $? -ne 0 ]; then
        echo $CONF
        exit 1
    fi

    sed -i "s/cassandra\.host=.*/cassandra\.host=$HOST/" $CONF
    sed -i "s/cassandra\.port=.*/cassandra\.port=$PORT/" $CONF

    echo $CONF
}

function config_scylladb() {

    BACKEND="scylladb"
    SERIALIZER="scylladb"
    STORE="hugegraph_${BACKEND}_${BUILD_ID}"
    HOST=${SCYLLADB_HOST}
    PORT=${SCYLLADB_PORT}

    CONF=`config_common $BACKEND $SERIALIZER $STORE`
    if [ $? -ne 0 ]; then
        echo $CONF
        exit 1
    fi

    sed -i "s/cassandra\.host=.*/cassandra\.host=$HOST/" $CONF
    sed -i "s/cassandra\.port=.*/cassandra\.port=$PORT/" $CONF

    echo $CONF
}

function config_rocksdb() {

    BACKEND="rocksdb"
    SERIALIZER="binary"
    STORE="hugegraph_$BUILD_ID"
    DATA_PATH="$STORE"

    CONF=`config_common $BACKEND $SERIALIZER $STORE`
    if [ $? -ne 0 ]; then
        echo $CONF
        exit 1
    fi

    mkdir -p hugegraph-test/$DATA_PATH
    sed -i "s/rocksdb\.data_path=.*/rocksdb\.data_path=$DATA_PATH/" $CONF
    sed -i "s/rocksdb\.wal_path=.*/rocksdb\.wal_path=$DATA_PATH/" $CONF

    echo $CONF
}

function config_mysql() {

    BACKEND="mysql"
    SERIALIZER="mysql"
    STORE="hugegraph_$BUILD_ID"
    JDBC_URL=${MYSQL_JDBC_URL}
    JDBC_USERNAME=${MYSQL_JDBC_USERNAME}

    CONF=`config_common $BACKEND $SERIALIZER $STORE`
    if [ $? -ne 0 ]; then
        echo $CONF
        exit 1
    fi

    sed -i "s/jdbc\.url=.*/jdbc\.url=$JDBC_URL/" $CONF
    sed -i "s/jdbc\.username=.*/jdbc\.username=$JDBC_USERNAME/" $CONF

    echo $CONF
}
