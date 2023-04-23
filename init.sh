#!/bin/bash
mkdir output
touch output/1
export JAVA_HOME=$ORACLEJDK_1_8_0_HOME

mvn -N install #only install root project

#$MAVEN_3_5_3_BIN/mvn -U deploy
