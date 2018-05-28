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

conf=$1
if [ $# -eq 0 ]; then
    conf=$CONF/hugegraph.properties
fi

cd $TOP

echo "Dumping HugeGraph Store($conf)..."

exec $JAVA -cp $LIB/hugegraph-dist-*.jar -Djava.ext.dirs=$LIB/ \
com.baidu.hugegraph.cmd.StoreDumper $conf $2 $3 $4
