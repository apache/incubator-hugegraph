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

# TODO: remove thie repo
readonly REPO_SNAPSHOT_PATH=http://10.14.139.8:8082/artifactory/star
readonly REPO_FILE_PATH=http://10.14.139.8:8082/artifactory/star-file
readonly REPO_BAIDU_LOCAL_PATH=http://maven.baidu-int.com/nexus/content/repositories/Baidu_Local
readonly PACKAGE_PATH=${REPO_SNAPSHOT_PATH}/com/baidu/hugegraph

readonly VER=3.6.3
{
  #command parameters
  cmd=$1
  ver=$2
}

case ${cmd} in
  "server")
    curl  ${REPO_FILE_PATH}/hugegraph/hugegraph-${ver}.tar.gz -o hugegraph-${ver}.tar.gz
  ;;
  "client")
    dest=store-client-${VER}
    mkdir ${dest}

	  curl  ${PACKAGE_PATH}/hg-store-client/${VER}/hg-store-client-${VER}.jar -o ./${dest}/hg-store-client-${VER}.jar
	  curl  ${PACKAGE_PATH}/hg-store-client/${VER}/hg-store-client-${VER}.pom -o ./${dest}/hg-store-client-pom.xml

	  curl  ${PACKAGE_PATH}/hg-store-grpc/${VER}/hg-store-grpc-${VER}.jar     -o ./${dest}/hg-store-grpc-${VER}.jar
	  curl  ${PACKAGE_PATH}/hg-store-grpc/${VER}/hg-store-grpc-${VER}.pom     -o ./${dest}/hg-store-grpc-pom.xml

	  curl  ${PACKAGE_PATH}/hg-store-common/${VER}/hg-store-common-${VER}.jar  -o ./${dest}/hg-store-common-${VER}.jar
	  curl  ${PACKAGE_PATH}/hg-store-common/${VER}/hg-store-common-${VER}.pom  -o ./${dest}/hg-store-common-pom.xml

	  curl  ${PACKAGE_PATH}/hg-pd-client/${VER}/hg-pd-client-${VER}.jar  -o ./${dest}/hg-pd-client-${VER}.jar
	  curl  ${PACKAGE_PATH}/hg-pd-client/${VER}/hg-pd-client-${VER}.pom  -o ./${dest}/hg-pd-client-pom.xml

	  curl  ${PACKAGE_PATH}/hg-pd-grpc/${VER}/hg-pd-grpc-${VER}.jar  -o ./${dest}/hg-pd-grpc-${VER}.jar
	  curl  ${PACKAGE_PATH}/hg-pd-grpc/${VER}/hg-pd-grpc-${VER}.pom  -o ./${dest}/hg-pd-grpc-pom.xml

	  curl  ${PACKAGE_PATH}/hg-pd-common/${VER}/hg-pd-common-${VER}.jar  -o ./${dest}/hg-pd-common-${VER}.jar
	  curl  ${PACKAGE_PATH}/hg-pd-common/${VER}/hg-pd-common-${VER}.pom  -o ./${dest}/hg-pd-common-pom.xml

	;;
  "store")
    dest=hugegraph-store-${VER}
    mkdir ${dest}
    curl  ${PACKAGE_PATH}/hugegraph-store/${VER}/hugegraph-store-${VER}.jar  -o ./${dest}/hugegraph-store-${VER}.jar -o hugegraph-store-${VER}.jar
  ;;
  "pd")
    dest=hugegraph-pd-${VER}
    mkdir ${dest}
    curl  ${PACKAGE_PATH}/hugegraph-pd/${VER}/hugegraph-pd-${VER}.jar  -o ./${dest}/hugegraph-pd-${VER}.jar -o hugegraph-pd-${VER}.jar
  ;;
  *)
    echo "./hg-get-release.sh [client|store|server|pd <ver>]"
  ;;
esac
