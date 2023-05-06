#!/bin/bash
mkdir output
touch output/1
export JAVA_HOME=$ORACLEJDK_1_8_0_HOME

readonly VER=3.6.3
readonly REPO_URL=http://10.14.139.8:8081/artifactory/star-local

$MAVEN_3_5_3_BIN/mvn -DremoveSnapshot=true  -DprocessAllModules=true -DgenerateBackupPoms=true versions:set
$MAVEN_3_5_3_BIN/mvn --settings ./settings.xml -Dmaven.test.skip=true -DaltDeploymentRepository=star-local::default::${REPO_URL} clean deploy
$MAVEN_3_5_3_BIN/mvn versions:revert


#------------------repo-----------------------
readonly FILE_NAME=hugegraph-pd-3.6.3.tar.gz
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
