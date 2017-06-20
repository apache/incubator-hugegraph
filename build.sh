#!/usr/bin/env bash
export MAVEN_HOME=/home/scmtools/buildkit/maven/apache-maven-3.3.9/
export JAVA_HOME=/home/scmtools/buildkit/java/jdk1.8.0_25/
export PATH=$JAVA_HOME/bin:$MAVEN_HOME/bin:$PATH
mvn clean package
