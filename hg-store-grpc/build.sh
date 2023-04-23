#!/bin/bash
mkdir output
touch output/1
export JAVA_HOME=$ORACLEJDK_1_8_0_HOME

#$MAVEN_3_5_3_BIN/mvn -U deploy

mvn -U install

