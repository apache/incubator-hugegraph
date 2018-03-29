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

if [ -n "$JAVA_HOME" ]; then
    JAVA="$JAVA_HOME"/bin/java
else
    JAVA=java
fi

conf=$1
if [ $# -eq 0 ]; then
    conf=$CONF/hugegraph.properties
fi

echo "Dumping HugeGraph Config($conf)..."

exec $JAVA -cp $LIB/hugegraph-dist-*.jar -Djava.ext.dirs=$LIB/ \
com.baidu.hugegraph.cmd.ConfDumper $conf
