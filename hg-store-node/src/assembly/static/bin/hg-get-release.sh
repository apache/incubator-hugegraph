#!/bin/bash
#ver 0.1.0 liyan75 on 2021/12/17
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
