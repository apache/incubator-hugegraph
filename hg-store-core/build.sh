#!/bin/bash

set -e


export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export MAVEN_HOME=/usr/share/maven
export PATH=$JAVA_HOME/bin:$MAVEN_HOME/bin:$PATH

mvn clean compile

rm -rf ~/tmp



