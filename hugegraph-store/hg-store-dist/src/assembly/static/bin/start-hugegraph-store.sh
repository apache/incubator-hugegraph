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
    while [ -h "$SOURCE" ]; do
        DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
        SOURCE="$(readlink "$SOURCE")"
        [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE"
    done
    echo "$( cd -P "$( dirname "$SOURCE" )" && pwd )"
}

BIN=$(abs_path)
TOP="$(cd "$BIN"/../ && pwd)"
CONF="$TOP/conf"
LIB="$TOP/lib"
PLUGINS="$TOP/plugins"
LOGS="$TOP/logs"
OUTPUT=${LOGS}/hugegraph-store-server.log
GITHUB="https://github.com"
PID_FILE="$BIN/pid"
arch=$(arch)

# TODO: replace it with uname -a?
echo "Current arch: ", ${arch}
#if [[ $arch =~ "aarch64" ]];then
#	  export LD_PRELOAD="$TOP/bin/libjemalloc_aarch64.so"
#else
export LD_PRELOAD="$TOP/bin/libjemalloc.so"
#fi

##pd/store max user processes, ulimit -u
# Reduce the maximum number of processes that can be opened by a normal dev/user
export PROC_LIMITN=1024
#export PROC_LIMITN=20480
##pd/store open files, ulimit -n
export FILE_LIMITN=1024
#export FILE_LIMITN=1024000

function check_evn_limit() {
    local limit_check=$(ulimit -n)
    if [ ${limit_check} -lt ${FILE_LIMITN} ]; then
        echo -e "${BASH_SOURCE[0]##*/}:${LINENO}:\E[1;32m ulimit -n 可以打开的最大文件描述符数太少,需要(${FILE_LIMITN})!! \E[0m"
        return 1
    fi
    limit_check=$(ulimit -u)
    if [ ${limit_check} -lt ${PROC_LIMITN} ]; then
        echo -e "${BASH_SOURCE[0]##*/}:${LINENO}:\E[1;32m ulimit -u  用户最大可用的进程数太少,需要(${PROC_LIMITN})!! \E[0m"
        return 2
    fi
    return 0
}

check_evn_limit
if [ $? != 0 ]; then
    exit 8
fi

if [ -z "$GC_OPTION" ];then
  GC_OPTION=""
fi
if [ -z "$USER_OPTION" ];then
  USER_OPTION=""
fi
if [ -z "$OPEN_TELEMETRY" ];then
  OPEN_TELEMETRY="false"
fi

while getopts "g:j:y:" arg; do
    case ${arg} in
        g) GC_OPTION="$OPTARG" ;;
        j) USER_OPTION="$OPTARG" ;;
        # Telemetry is used to collect metrics, traces and logs
        y) OPEN_TELEMETRY="$OPTARG" ;;
        ?) echo "USAGE: $0 [-g g1] [-j xxx] [-y true|false]" && exit 1 ;;
    esac
done




. "$BIN"/util.sh

ensure_path_writable "$LOGS"
ensure_path_writable "$PLUGINS"

# The maximum and minimum heap memory that service can use (for production env set it 36GB)
MAX_MEM=$((2 * 1024))
MIN_MEM=$((1 * 512))
EXPECT_JDK_VERSION=11

# Change to $BIN's parent
cd ${TOP} || exit

# Find Java
if [ "$JAVA_HOME" = "" ]; then
    JAVA="java"
else
    JAVA="$JAVA_HOME/bin/java"
fi

# check jdk version
JAVA_VERSION=$($JAVA -version 2>&1 | awk 'NR==1{gsub(/"/,""); print $3}'  | awk -F'_' '{print $1}')
if [[ $? -ne 0 || $JAVA_VERSION < $EXPECT_JDK_VERSION ]]; then
    echo "Please make sure that the JDK is installed and the version >= $EXPECT_JDK_VERSION"  >> ${OUTPUT}
    exit 1
fi

# Set Java options
if [ "$JAVA_OPTIONS" = "" ]; then
    XMX=$(calc_xmx $MIN_MEM $MAX_MEM)
    if [ $? -ne 0 ]; then
        echo "Failed to start HugeGraphStoreServer, requires at least ${MIN_MEM}m free memory" \
             >> ${OUTPUT}
        exit 1
    fi
     JAVA_OPTIONS="-Xms${MIN_MEM}m -Xmx${XMX}m -XX:MetaspaceSize=256M  -XX:+UseG1GC  -XX:+ParallelRefProcEnabled -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=${LOGS} ${USER_OPTION} "
    # JAVA_OPTIONS="-Xms${MIN_MEM}m -Xmx${XMX}m -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=${LOGS} ${USER_OPTION}"

    # Rolling out detailed GC logs
    JAVA_OPTIONS="${JAVA_OPTIONS} -Xlog:gc=info:file=./logs/gc.log:tags,uptime,level:filecount=3,filesize=100m "
fi

# Using G1GC as the default garbage collector (Recommended for large memory machines)
case "$GC_OPTION" in
    g1)
        echo "Using G1GC as the default garbage collector"
        JAVA_OPTIONS="${JAVA_OPTIONS} -XX:+UseG1GC -XX:+ParallelRefProcEnabled \
                      -XX:InitiatingHeapOccupancyPercent=50 -XX:G1RSetUpdatingPauseTimePercent=5"
        ;;
    "") ;;
    *)
        echo "Unrecognized gc option: '$GC_OPTION', only support 'g1' now" >> ${OUTPUT}
        exit 1
esac

JVM_OPTIONS="-Dlog4j.configurationFile=${CONF}/log4j2.xml -Dfastjson.parser.safeMode=true -Djava.util.logging.manager=org.apache.logging.log4j.jul.LogManager"

if [ "${OPEN_TELEMETRY}" == "true" ]; then
    OT_JAR="opentelemetry-javaagent.jar"
    OT_JAR_PATH="${PLUGINS}/${OT_JAR}"

    if [[ ! -e "${OT_JAR_PATH}" ]]; then
        echo "## Downloading ${OT_JAR}..."
        download "${PLUGINS}" \
            "${GITHUB}/open-telemetry/opentelemetry-java-instrumentation/releases/download/v2.1.0/${OT_JAR}"

        if [[ ! -e "${OT_JAR_PATH}" ]]; then
            echo "## Error: Failed to download ${OT_JAR}." >>${OUTPUT}
            exit 1
        fi
    fi

    # Note: remember update it if we change the jar
    expected_md5="e3bcbbe8ed9b6d840fa4c333b36f369f"
    actual_md5=$(md5sum "${OT_JAR_PATH}" | awk '{print $1}')

    if [[ "${expected_md5}" != "${actual_md5}" ]]; then
        echo "## Error: MD5 checksum verification failed for ${OT_JAR_PATH}." >>${OUTPUT}
        echo "## Tips: Remove the file and try again." >>${OUTPUT}
        exit 1
    fi

    # Note: check carefully if multi "javeagent" params are set
    export JAVA_TOOL_OPTIONS="-javaagent:${PLUGINS}/${OT_JAR}"
    export OTEL_TRACES_EXPORTER=otlp
    export OTEL_METRICS_EXPORTER=none
    export OTEL_LOGS_EXPORTER=none
    export OTEL_EXPORTER_OTLP_TRACES_PROTOCOL=grpc
    # 127.0.0.1:4317 is the port of otel-collector running in Docker located in
    # 'hugegraph-server/hugegraph-dist/docker/example/docker-compose-trace.yaml'.
    # Make sure the otel-collector is running before starting HugeGraphStore.
    export OTEL_EXPORTER_OTLP_TRACES_ENDPOINT=http://127.0.0.1:4317
    export OTEL_RESOURCE_ATTRIBUTES=service.name=store
fi

#if [ "${JMX_EXPORT_PORT}" != "" ] && [ ${JMX_EXPORT_PORT} -ne 0 ] ; then
#  JAVA_OPTIONS="${JAVA_OPTIONS} -javaagent:${LIB}/jmx_prometheus_javaagent-0.16.1.jar=${JMX_EXPORT_PORT}:${CONF}/jmx_exporter.yml"
#fi

if [ $(ps -ef|grep -v grep| grep java|grep -cE ${CONF}) -ne 0 ]; then
   echo "HugeGraphStoreServer is already running..."
   exit 0
fi

echo "Starting HG-StoreServer..."

exec ${JAVA} -Dname="HugeGraphStore" ${JVM_OPTIONS} ${JAVA_OPTIONS} -jar \
    -Dspring.config.location=${CONF}/application.yml \
    ${LIB}/hg-store-node-*.jar >> ${OUTPUT} 2>&1 &

PID="$!"
# Write pid to file
echo "$PID" > "$PID_FILE"
echo "[+pid] $PID"
