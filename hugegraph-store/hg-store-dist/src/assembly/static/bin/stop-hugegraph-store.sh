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

abs_path() {
    SOURCE="${BASH_SOURCE[0]}"
    while [ -h "$SOURCE" ]; do
        DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
        SOURCE="$(readlink "$SOURCE")"
        [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE"
    done
    echo "$( cd -P "$( dirname "$SOURCE" )" && pwd )"
}

BIN=$(abs_path)
TOP="$(cd $BIN/../ && pwd)"

. "$BIN"/util.sh

PID_FILE=$BIN/pid
SERVER_SHUTDOWN_TIMEOUT_S=30

if [ ! -f ${PID_FILE} ]; then
    echo "The pid file $PID_FILE doesn't exist"
    exit 0
fi

PID=`cat $PID_FILE`
kill_process_and_wait "HugeGraphStoreServer" "$PID" "$SERVER_SHUTDOWN_TIMEOUT_S"

if [ $? -eq 0 ]; then
    rm "$PID_FILE"
fi
