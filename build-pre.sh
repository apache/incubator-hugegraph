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

mkdir output
touch output/1
export JAVA_HOME=$ORACLEJDK_1_8_0_HOME

readonly VER=3.6.3
readonly REPO_URL=http://10.14.139.8:8081/artifactory/star-local

$MAVEN_3_5_3_BIN/mvn -DremoveSnapshot=true  -DprocessAllModules=true -DgenerateBackupPoms=true versions:set
$MAVEN_3_5_3_BIN/mvn --settings ./settings.xml -Dmaven.test.skip=true -DaltDeploymentRepository=star-local::default::${REPO_URL} clean deploy
$MAVEN_3_5_3_BIN/mvn versions:revert

#------------------repo-server-----------------------
readonly FILE_NAME=hugegraph-store-3.6.3.tar.gz
readonly REPO_URL_FILE=http://10.14.139.8:8081/artifactory/star-file

localFilePath=dist/${FILE_NAME}
targetFolder="${REPO_URL_FILE}/dist/$(date '+%Y-%m-%d')/"
artifactoryUser="admin"
artifactoryPassword="JFrog12345"

md5Value="$(md5sum "$localFilePath")"
md5Value="${md5Value:0:32}"
sha1Value="$(sha1sum "$localFilePath")"
sha1Value="${sha1Value:0:40}"
sha256Value="$(sha256sum "$localFilePath")"
sha256Value="${sha256Value:0:65}"

#curl -X PUT -u admin:JFrog12345 -T ${localFilePath} "${REPO_URL_FILE}/dist/${data_folder}/"
echo "INFO: Uploading $localFilePath to $targetFolder"
curl -i -X PUT -u "$artifactoryUser:$artifactoryPassword" \
 -H "X-Checksum-Md5: $md5Value" \
 -H "X-Checksum-Sha1: $sha1Value" \
 -H "X-Checksum-Sha256: $sha256Value" \
 -T "$localFilePath" \
 "$targetFolder"


 #------------------repo-client-----------------------
 # TODO: remove thie repo
readonly REPO_SNAPSHOT_PATH=http://10.14.139.8:8082/artifactory/star
readonly REPO_FILE_PATH=http://10.14.139.8:8082/artifactory/star-file
readonly REPO_BAIDU_LOCAL_PATH=http://maven.baidu-int.com/nexus/content/repositories/Baidu_Local
readonly PACKAGE_PATH=${REPO_SNAPSHOT_PATH}/com/baidu/hugegraph

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

tar -cvf ${dest}.tar  ${dest}

localFilePath=${dest}.tar
targetFolder="${REPO_URL_FILE}/dist/$(date '+%Y-%m-%d')/"
artifactoryUser="admin"
artifactoryPassword="JFrog12345"

md5Value="$(md5sum "$localFilePath")"
md5Value="${md5Value:0:32}"
sha1Value="$(sha1sum "$localFilePath")"
sha1Value="${sha1Value:0:40}"
sha256Value="$(sha256sum "$localFilePath")"
sha256Value="${sha256Value:0:65}"

#curl -X PUT -u admin:JFrog12345 -T ${localFilePath} "${REPO_URL_FILE}/dist/${data_folder}/"
echo "INFO: Uploading $localFilePath to $targetFolder"
curl -i -X PUT -u "$artifactoryUser:$artifactoryPassword" \
 -H "X-Checksum-Md5: $md5Value" \
 -H "X-Checksum-Sha1: $sha1Value" \
 -H "X-Checksum-Sha256: $sha256Value" \
 -T "$localFilePath" \
 "$targetFolder"