#!/bin/bash

abs_path() {
    SOURCE="${BASH_SOURCE[0]}"
    while [ -h "$SOURCE" ]; do
        DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
        SOURCE="$(readlink "$SOURCE")"
        [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE"
    done
    echo "$( cd -P "$( dirname "$SOURCE" )" && pwd )"
}

BIN=`abs_path`
TOP="$(cd $BIN/../ && pwd)"
CONF=$TOP/conf
LIB=$TOP/lib

if [ -n "$JAVA_HOME" ]; then
    JAVA="$JAVA_HOME"/bin/java
else
    JAVA=java
fi

cd $TOP

echo "Initing HugeGraph Store..."

exec $JAVA -cp $LIB/hugegraph-dist-*.jar -Djava.ext.dirs=$LIB/ \
com.baidu.hugegraph.cmd.InitStore $CONF/gremlin-server.yaml | grep "com.baidu.hugegraph"
