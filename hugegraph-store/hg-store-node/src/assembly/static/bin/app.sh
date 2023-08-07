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

#ver 0.1.0 liyan75 on 2021/10/08
#readonly CUR_SHELL=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
readonly CUR_SHELL_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
readonly CUR_SHELL_NAME=$(basename ${BASH_SOURCE})

readonly LOG_PATH=${CUR_SHELL_DIR}/stdout.log
readonly APP_JAR_PATH=./hg-store-node-1.0-SNAPSHOT-spring-boot.jar

{
    #command parameters
    CMD=$1
}

start(){
  nohup java -jar ${APP_JAR_PATH} >> ${LOG_PATH} 2>&1 &
  pid=$!
  echo $pid>>${CUR_SHELL_DIR}/app.pid
}

stop(){
  while read pid; do
    kill ${pid}
  done <${CUR_SHELL_DIR}/app.pid
  echo >${CUR_SHELL_DIR}/app.pid
}

help(){
    _log "syntax: /bin/bash $CUR_SHELL_NAME [ start | stop ]"
    exit 1
}
function _log(){
    echo -e "$(_getTime) : $1" 2>&1
}
function _err(){
    echo -e "$(_getTime) : ERROR: $1" 1>&2
    return $2;
}
function _getTime(){
    local res=`date +"%Y/%m/%d %H:%M:%S"`
    echo $res
}

#entrance
case ${CMD} in
    "start")
        #init
        start;;
    "stop")
        stop;;
    "alive")
        alive;;
    *)
        help;;
esac