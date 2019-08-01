#!/bin/bash

abs_path() {
    SOURCE="${BASH_SOURCE[0]}"
    while [ -h "$SOURCE" ]; do
        DIR="$(cd -P "$(dirname "$SOURCE")" && pwd)"
        SOURCE="$(readlink "$SOURCE")"
        [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE"
    done
    echo "$(cd -P "$(dirname "$SOURCE")" && pwd)"
}

if [[ $# -lt 3 ]]; then
    echo "USAGE: $0 GREMLIN_SERVER_CONF REST_SERVER_CONF OPEN_SECURITY_CHECK"
    echo " e.g.: $0 conf/gremlin-server.yaml conf/rest-server.properties true"
    exit 1;
fi

GREMLIN_SERVER_CONF="$1"
REST_SERVER_CONF="$2"
OPEN_SECURITY_CHECK="$3"
GC_OPTION="$4"

BIN=$(abs_path)
TOP="$(cd $BIN/../ && pwd)"
CONF="$TOP/conf"
LIB="$TOP/lib"
EXT="$TOP/ext"
PLUGINS="$TOP/plugins"
LOG="$TOP/logs"
OUTPUT=${LOG}/hugegraph-server.log

export HUGEGRAPH_HOME="$TOP"
. ${BIN}/util.sh

ensure_path_writable $LOG
ensure_path_writable $PLUGINS

# The maximum and minium heap memory that service can use
MAX_MEM=$((32 * 1024))
MIN_MEM=$((1 * 512))
EXPECT_JDK_VERSION=1.8

# Add the slf4j-log4j12 binding
CP=$(find -L $LIB -name 'log4j-slf4j-impl*.jar' | sort | tr '\n' ':')
# Add the jars in lib that start with "hugegraph"
CP="$CP":$(find -L $LIB -name 'hugegraph*.jar' | sort | tr '\n' ':')
# Add the remaining jars in lib.
CP="$CP":$(find -L $LIB -name '*.jar' \
                \! -name 'hugegraph*' \
                \! -name 'log4j-slf4j-impl*.jar' | sort | tr '\n' ':')
# Add the jars in ext (at any subdirectory depth)
CP="$CP":$(find -L $EXT -name '*.jar' | sort | tr '\n' ':')
# Add the jars in plugins (at any subdirectory depth)
CP="$CP":$(find -L $PLUGINS -name '*.jar' | sort | tr '\n' ':')

# (Cygwin only) Use ; classpath separator and reformat paths for Windows ("C:\foo")
[[ $(uname) = CYGWIN* ]] && CP="$(cygpath -p -w "$CP")"

export CLASSPATH="${CLASSPATH:-}:$CP"

# Change to $BIN's parent
cd ${TOP}

# Find Java
if [ "$JAVA_HOME" = "" ]; then
    JAVA="java -server"
else
    JAVA="$JAVA_HOME/bin/java -server"
fi

JAVA_VERSION=$($JAVA -version 2>&1 | awk 'NR==1{gsub(/"/,""); print $3}' \
              | awk -F'_' '{print $1}')
if [[ $? -ne 0 || $JAVA_VERSION < $EXPECT_JDK_VERSION ]]; then
    echo "Please make sure that the JDK is installed and the version >= $EXPECT_JDK_VERSION" \
         >> ${OUTPUT}
    exit 1
fi

# Set Java options
if [ "$JAVA_OPTIONS" = "" ]; then
    XMX=$(calc_xmx $MIN_MEM $MAX_MEM)
    if [ $? -ne 0 ]; then
        echo "Failed to start HugeGraphServer, requires at least ${MIN_MEM}m free memory" \
             >> ${OUTPUT}
        exit 1
    fi
    JAVA_OPTIONS="-Xms${MIN_MEM}m -Xmx${XMX}m -javaagent:$LIB/jamm-0.3.0.jar"
    
    # Rolling out detailed GC logs
    #JAVA_OPTIONS="${JAVA_OPTIONS} -XX:+UseGCLogFileRotation -XX:GCLogFileSize=10M -XX:NumberOfGCLogFiles=3 \
    #              -Xloggc:./logs/gc.log -XX:+PrintHeapAtGC -XX:+PrintGCDetails -XX:+PrintGCDateStamps"
fi

# Using G1GC as the default garbage collector (Recommended for large memory machines)
case "$GC_OPTION" in
    g1)
        echo "Using G1GC as the default garbage collector"
        JAVA_OPTIONS="${JAVA_OPTIONS} -XX:+UseG1GC -XX:+ParallelRefProcEnabled \
                      -XX:InitiatingHeapOccupancyPercent=50 -XX:G1RSetUpdatingPauseTimePercent=5"
        ;;
    *)
        echo "Unrecognized gc option: '$GC_OPTION', only support 'g1' now" >> ${OUTPUT}
        exit 1
esac

JVM_OPTIONS="-Dlog4j.configurationFile=${CONF}/log4j2.xml"
if [[ ${OPEN_SECURITY_CHECK} == "true" ]]; then
    JVM_OPTIONS="${JVM_OPTIONS} -Djava.security.manager=com.baidu.hugegraph.security.HugeSecurityManager"
fi

# Turn on security check
exec ${JAVA} -Dname="HugeGraphServer" ${JVM_OPTIONS} ${JAVA_OPTIONS} \
     -cp ${CLASSPATH}: com.baidu.hugegraph.dist.HugeGraphServer ${GREMLIN_SERVER_CONF} ${REST_SERVER_CONF} \
     >> ${OUTPUT} 2>&1
