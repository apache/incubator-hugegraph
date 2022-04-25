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

DEFAULT_JAVA_OPTIONS=""
JAVA_VERSION=$($JAVA -version 2>&1 | awk 'NR==1{gsub(/"/,""); print $3}' \
              | awk -F'_' '{print $1}')
if [[ $? -eq 0 && $JAVA_VERSION >  "1.9" ]]; then
      DEFAULT_JAVA_OPTIONS="--add-exports=java.base/jdk.internal.reflect=ALL-UNNAMED"
fi

echo "Initializing HugeGraph Store..."

CP=$(find "${LIB}" "${PLUGINS}" -name "*.jar"  | tr "\n" ":")
$JAVA -cp $CP ${DEFAULT_JAVA_OPTIONS} \
com.baidu.hugegraph.cmd.InitStore "${CONF}"/rest-server.properties

echo "Initialization finished."
