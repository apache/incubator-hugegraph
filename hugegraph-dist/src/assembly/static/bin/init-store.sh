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
CONF=$BIN/../conf
LIB=$BIN/../lib

echo "Initing HugeGraph Store..."

if [ -n "$JAVA_HOME" ]; then
    JAVA="$JAVA_HOME"/bin/java
else
    JAVA=java
fi

exec $JAVA -cp $LIB/hugegraph-dist-*.jar -Djava.ext.dirs=$LIB/ \
com.baidu.hugegraph.dist.InitStore $CONF/gremlin-server.yaml | grep INFO