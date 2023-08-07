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

#ver 0.1.0 liyan75 on 2021/12/17
readonly REPO_SNAPSHOT_PATH=http://10.14.139.8:8082/artifactory/star-snapshot
readonly REPO_FILE_PATH=http://10.14.139.8:8082/artifactory/star-file
readonly REPO_BAIDU_LOCAL_PATH=http://maven.baidu-int.com/nexus/content/repositories/Baidu_Local
{
  #command parameters
  cmd=$1
  ver=$2
}

case ${cmd} in
  "server")
    if [ -z "${ver}" ]; then
      echo "The argument <ver> is empty. e.g. 0.13.0"
      exit 1;
    fi
    curl  ${REPO_FILE_PATH}/hugegraph/hugegraph-${ver}.tar.gz -o hugegraph-${ver}.tar.gz
  ;;
  "client")
	  curl  ${REPO_SNAPSHOT_PATH}/com/baidu/hugegraph/hg-store-client/1.0-SNAPSHOT/hg-store-client-1.0-SNAPSHOT.jar -o hg-store-client-1.0-SNAPSHOT.jar
	  curl  ${REPO_SNAPSHOT_PATH}/com/baidu/hugegraph/hg-store-grpc/1.0-SNAPSHOT/hg-store-grpc-1.0-SNAPSHOT.jar -o hg-store-grpc-1.0-SNAPSHOT.jar
	  curl  ${REPO_BAIDU_LOCAL_PATH}/com/baidu/hugegraph/hg-store-common/1.0.1/hg-store-common-1.0.1.jar -o hg-store-term-1.0.1.jar
	;;
  "node")
    curl  ${REPO_SNAPSHOT_PATH}/com/baidu/hugegraph/hg-store-node/1.0-SNAPSHOT/hg-store-node-1.0-SNAPSHOT-spring-boot.jar -o hg-store-node-1.0-SNAPSHOT-spring-boot.jar
  ;;
  *)
    echo "./hg-update.sh [client|node|server <ver>]"
  ;;
esac
