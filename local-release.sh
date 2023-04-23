#!/bin/bash
readonly VER=3.6.3

#mvn -DnewVersion=${VER}-SNAPSHOT  -DprocessAllModules=true -DgenerateBackupPoms=false versions:set

mvn -DremoveSnapshot=true  -DprocessAllModules=true -DgenerateBackupPoms=true versions:set
mvn --settings ./settings.xml -Dmaven.test.skip=true  clean install
mvn versions:revert