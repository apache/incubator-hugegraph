#!/bin/bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with this
# work for additional information regarding copyright ownership. The ASF
# licenses this file to You under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
#

if [ -z "$GC_OPTION" ];then
  GC_OPTION=""
fi
if [ -z "$USER_OPTION" ];then
  USER_OPTION=""
fi

while getopts "g:j:v" arg; do
    case ${arg} in
        g) GC_OPTION="$OPTARG" ;;
        j) USER_OPTION="$OPTARG" ;;
        ?) echo "USAGE: $0 [-g g1] [-j xxx] [-v]" && exit 1 ;;
    esac
done

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
LOGS="$TOP/logs"
OUTPUT=${LOGS}/hugegraph-pd-stdout.log
PID_FILE="$BIN/pid"

. "$BIN"/util.sh

mkdir -p ${LOGS}

# The maximum and minium heap memory that service can use
MAX_MEM=$((32 * 1024))
MIN_MEM=$((1 * 512))
EXPECT_JDK_VERSION=11

# Change to $BIN's parent
cd "${TOP}" || exit

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
        echo "Failed to start HugeGraphPDServer, requires at least ${MIN_MEM}m free memory" \
             >> ${OUTPUT}
        exit 1
    fi
    JAVA_OPTIONS="-Xms${MIN_MEM}m -Xmx${XMX}m -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=${LOGS} ${USER_OPTION}"

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
    "") ;;
    *)
        echo "Unrecognized gc option: '$GC_OPTION', only support 'g1' now" >> ${OUTPUT}
        exit 1
esac

#if [ "${JMX_EXPORT_PORT}" != "" ] && [ ${JMX_EXPORT_PORT} -ne 0 ] ; then
#  JAVA_OPTIONS="${JAVA_OPTIONS} -javaagent:${LIB}/jmx_prometheus_javaagent-0.16.1.jar=${JMX_EXPORT_PORT}:${CONF}/jmx_exporter.yml"
#fi
if [ $(ps -ef|grep -v grep| grep java|grep -cE ${CONF}) -ne 0 ]; then
   echo "HugeGraphPDServer is already running..."
   exit 0
fi
echo "Starting HugeGraphPDServer..."

# Turn on security check
exec ${JAVA} -Dname="HugeGraphPD" ${JAVA_OPTIONS} -jar \
    -Dspring.config.location=${CONF}/application.yml ${LIB}/hg-pd-service-*.jar >> ${OUTPUT} 2>&1 &

PID="$!"
# Write pid to file
echo "$PID" > "$PID_FILE"
echo "[+pid] $PID"
