#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
function abs_path() {
    SOURCE="${BASH_SOURCE[0]}"
    while [[ -h "$SOURCE" ]]; do
        DIR="$(cd -P "$(dirname "$SOURCE")" && pwd)"
        SOURCE="$(readlink "$SOURCE")"
        [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE"
    done
    cd -P "$(dirname "$SOURCE")" && pwd
}

if [[ $# -lt 3 ]]; then
    echo "USAGE: $0 GREMLIN_SERVER_CONF REST_SERVER_CONF OPEN_SECURITY_CHECK"
    echo " e.g.: $0 conf/gremlin-server.yaml conf/rest-server.properties true"
    exit 1
fi

BIN=$(abs_path)
TOP="$(cd "$BIN"/../ && pwd)"
CONF="$TOP/conf"
LIB="$TOP/lib"
EXT="$TOP/ext"
PLUGINS="$TOP/plugins"
LOGS="$TOP/logs"
OUTPUT=${LOGS}/hugegraph-server.log

export HUGEGRAPH_HOME="$TOP"
. "${BIN}"/util.sh

GREMLIN_SERVER_CONF="$1"
REST_SERVER_CONF="$2"
OPEN_SECURITY_CHECK="$3"

if [[ $# -eq 3 ]]; then
    USER_OPTION=""
    GC_OPTION=""
elif [[ $# -eq 4 ]]; then
    USER_OPTION="$4"
    GC_OPTION=""
elif [[ $# -eq 5 ]]; then
    USER_OPTION="$4"
    GC_OPTION="$5"
fi

ensure_path_writable "$LOGS"
ensure_path_writable "$PLUGINS"

# The maximum and minimum heap memory that service can use
MAX_MEM=$((32 * 1024))
MIN_MEM=$((1 * 512))
MIN_JAVA_VERSION=8

# download binary file
if [[ ! -e "${CONF}/hugegraph-server.keystore" ]]; then
    download "${CONF}" "https://github.com/apache/hugegraph-doc/raw/binary-1.0/dist/server/hugegraph-server.keystore"
fi

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
cd "${TOP}" || exit 1

# Find java & enable server option
if [ "$JAVA_HOME" = "" ]; then
    JAVA="java -server"
else
    JAVA="$JAVA_HOME/bin/java -server"
fi

JAVA_VERSION=$($JAVA -version 2>&1 | head -1 | cut -d'"' -f2 | sed 's/^1\.//' | cut -d'.' -f1)
if [[ $? -ne 0 || $JAVA_VERSION -lt $MIN_JAVA_VERSION ]]; then
    echo "Make sure the JDK is installed and the version >= $MIN_JAVA_VERSION, current is $JAVA_VERSION" \
         >> "${OUTPUT}"
    exit 1
fi

# Set Java options
if [ "$JAVA_OPTIONS" = "" ]; then
    XMX=$(calc_xmx $MIN_MEM $MAX_MEM)
    if [ $? -ne 0 ]; then
        echo "Failed to start HugeGraphServer, requires at least ${MIN_MEM}MB free memory" >> "${OUTPUT}"
        exit 1
    fi
    JAVA_OPTIONS="-Xms${MIN_MEM}m -Xmx${XMX}m -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=${LOGS} ${USER_OPTION}"

    # Rolling out detailed GC logs
    #JAVA_OPTIONS="${JAVA_OPTIONS} -XX:+UseGCLogFileRotation -XX:GCLogFileSize=10M -XX:NumberOfGCLogFiles=3 \
    #              -Xloggc:./logs/gc.log -XX:+PrintHeapAtGC -XX:+PrintGCDetails -XX:+PrintGCDateStamps"
fi

if [[ $JAVA_VERSION -gt 9 ]]; then
    JAVA_OPTIONS="${JAVA_OPTIONS} --add-exports=java.base/jdk.internal.reflect=ALL-UNNAMED \
                                  --add-modules=jdk.unsupported \
                                  --add-exports=java.base/sun.nio.ch=ALL-UNNAMED "
fi

# Using G1GC as the default garbage collector (Recommended for large memory machines)
# mention: zgc is only available on ARM-Mac with java > 13
case "$GC_OPTION" in
    g1|G1|g1gc)
        echo "Using G1GC as the default garbage collector"
        JAVA_OPTIONS="${JAVA_OPTIONS} -XX:+UseG1GC -XX:+ParallelRefProcEnabled \
                                      -XX:InitiatingHeapOccupancyPercent=50 \
                                      -XX:G1RSetUpdatingPauseTimePercent=5"
        ;;
    zgc|ZGC)
        echo "Using ZGC as the default garbage collector (Only support Java 11+)"
        JAVA_OPTIONS="${JAVA_OPTIONS} -XX:+UseZGC -XX:+UnlockExperimentalVMOptions \
                                      -XX:ConcGCThreads=2 -XX:ParallelGCThreads=6 \
                                      -XX:ZCollectionInterval=120 -XX:ZAllocationSpikeTolerance=5 \
                                      -XX:+UnlockDiagnosticVMOptions -XX:-ZProactive"
        ;;
    "") ;;
    *)
        echo "Unrecognized gc option: '$GC_OPTION', only support 'G1/ZGC' now" >> ${OUTPUT}
        exit 1
esac

JVM_OPTIONS="-Dlog4j.configurationFile=${CONF}/log4j2.xml"
if [[ ${OPEN_SECURITY_CHECK} == "true" ]]; then
    JVM_OPTIONS="${JVM_OPTIONS} -Djava.security.manager=org.apache.hugegraph.security.HugeSecurityManager"
fi

# Turn on security check
exec ${JAVA} -Dname="HugeGraphServer" ${JVM_OPTIONS} ${JAVA_OPTIONS} -cp ${CLASSPATH}: \
    org.apache.hugegraph.dist.HugeGraphServer ${GREMLIN_SERVER_CONF} ${REST_SERVER_CONF} \
    >> ${OUTPUT} 2>&1
