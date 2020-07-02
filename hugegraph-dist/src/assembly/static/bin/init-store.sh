#!/bin/bash

abs_path() {
    SOURCE="${BASH_SOURCE[0]}"
    while [[ -h "$SOURCE" ]]; do
        DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
        SOURCE="$(readlink "$SOURCE")"
        [[ ${SOURCE} != /* ]] && SOURCE="$DIR/$SOURCE"
    done
    echo "$( cd -P "$( dirname "$SOURCE" )" && pwd )"
}

BIN=`abs_path`
TOP="$(cd ${BIN}/../ && pwd)"
CONF="$TOP/conf"
LIB="$TOP/lib"
PLUGINS="$TOP/plugins"

. ${BIN}/util.sh

ensure_path_writable ${PLUGINS}

if [[ -n "$JAVA_HOME" ]]; then
    JAVA="$JAVA_HOME"/bin/java
    EXT="$JAVA_HOME/jre/lib/ext:$LIB:$PLUGINS"
else
    JAVA=java
    EXT="$LIB:$PLUGINS"
fi

cd ${TOP}

echo "Initializing HugeGraph Store..."

${JAVA} -cp ${LIB}/hugegraph-dist-*.jar -Djava.ext.dirs=${LIB}:${PLUGINS} \
    com.baidu.hugegraph.cmd.InitStore ${CONF}/rest-server.properties

echo "Initialization finished."
