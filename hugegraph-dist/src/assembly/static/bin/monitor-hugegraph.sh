#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
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

BIN=`abs_path`
TOP="$(cd $BIN/../ && pwd)"

. $BIN/util.sh

SERVER_URL=`read_property "$TOP/conf/rest-server.properties" "restserver.url"`
DETECT_URL="$SERVER_URL/versions"
PROC_NAME="HugeGraphServer"

LOG_DIR="$TOP/logs"
MONITOR_LOG="$LOG_DIR/monitor.log"

if [ ! -d $LOG_DIR ]; then
    mkdir $LOG_DIR
fi

function record_monitor_log() {
    echo `date '+%Y-%m-%d %H:%M:%S'`, $1 >> $MONITOR_LOG
}

function restart_server() {
    local stop_old=$1
    if [ "$stop_old" == "true" ]; then
        # Don't remove monitor
        $BIN/stop-hugegraph.sh false
    fi
    record_monitor_log "Ready to restart $PROC_NAME"
    # Don't add monitor again
    $BIN/start-hugegraph.sh -m false
    if [ $? -ne 0 ]; then
        record_monitor_log "Failed to restart $PROC_NAME"
        exit 1
    fi
    # Record the new process number and restart time
    record_monitor_log "Restarted $PROC_NAME"
}

process_num $PROC_NAME
NUMBER=$?

STATUS=`curl -I -s -w "%{http_code}" -o /dev/null $DETECT_URL`
if [ $STATUS -ne 200 ]; then
    sleep 5
    STATUS=`curl -I -s -w "%{http_code}" -o /dev/null $DETECT_URL`
fi

# There is no process
if [ $NUMBER -eq 0 ]; then
    restart_server false
else
    # The process is running but request fails
    if [ $STATUS -ne 200 ]; then
        record_monitor_log "$PROC_NAME is running but request fails, ready to stop and restart it"
        restart_server true
    fi
fi
