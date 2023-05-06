#!/bin/bash
readonly VER=3.6.3
readonly REPO_URL=http://10.14.139.8:8081/artifactory/star-local
#mvn -DnewVersion=${VER}-SNAPSHOT  -DprocessAllModules=true -DgenerateBackupPoms=false versions:set

./mvnw -DremoveSnapshot=true  -DprocessAllModules=true -DgenerateBackupPoms=true versions:set
./mvnw --settings ./settings.xml -Dmaven.test.skip=true -DaltDeploymentRepository=star-local::default::${REPO_URL} clean deploy
./mvnw versions:revert
