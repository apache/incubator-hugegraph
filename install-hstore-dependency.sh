#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

if [ -z $JAVA_HOME ]; then
   export JAVA_HOME=/home/scmtools/buildkit/java/jdk1.8.0_25
   export PATH=$JAVA_HOME/bin:$PATH
fi

if [ -z $MAVEN_HOME ]; then
   export MAVEN_HOME=/home/scmtools/buildkit/maven/apache-maven-3.3.9
   export PATH=$MAVEN_HOME/bin:$PATH
fi

TRAVIS_DIR=./hugegraph-dist/src/assembly/travis/lib
mvn install:install-file -Dfile=$TRAVIS_DIR/hg-pd-client-3.0.0.jar -DgroupId=com.baidu.hugegraph -DartifactId=hg-pd-client -Dversion=3.0.0 -Dpackaging=jar  -DpomFile=$TRAVIS_DIR/hg-pd-client-pom.xml
mvn install:install-file -Dfile=$TRAVIS_DIR/hg-pd-common-3.0.0.jar -DgroupId=com.baidu.hugegraph -DartifactId=hg-pd-common -Dversion=3.0.0 -Dpackaging=jar -DpomFile=$TRAVIS_DIR/hg-pd-common-pom.xml
mvn install:install-file -Dfile=$TRAVIS_DIR/hg-pd-grpc-3.0.0.jar -DgroupId=com.baidu.hugegraph -DartifactId=hg-pd-grpc -Dversion=3.0.0 -Dpackaging=jar -DpomFile=$TRAVIS_DIR/hg-pd-grpc-pom.xml
mvn install:install-file -Dfile=$TRAVIS_DIR/hg-store-client-3.0.0.jar -DgroupId=com.baidu.hugegraph -DartifactId=hg-store-client -Dversion=3.0.0 -Dpackaging=jar -DpomFile=$TRAVIS_DIR/hg-store-client-pom.xml
mvn install:install-file -Dfile=$TRAVIS_DIR/hg-store-grpc-3.0.0.jar -DgroupId=com.baidu.hugegraph -DartifactId=hg-store-grpc -Dversion=3.0.0 -Dpackaging=jar -DpomFile=$TRAVIS_DIR/hg-store-grpc-pom.xml
mvn install:install-file -Dfile=$TRAVIS_DIR/hg-store-term-3.0.0.jar -DgroupId=com.baidu.hugegraph -DartifactId=hg-store-term -Dversion=3.0.0 -Dpackaging=jar -DpomFile=$TRAVIS_DIR/hg-store-term-pom.xml
mvn install:install-file -Dfile=$TRAVIS_DIR/hugegraph-computer-0.1.1.pom -DgroupId=com.baidu.hugegraph -DartifactId=hugegraph-computer -Dversion=0.1.1 -Dpackaging=pom
mvn install:install-file -Dfile=$TRAVIS_DIR/computer-driver-0.1.1.jar -DgroupId=com.baidu.hugegraph -DartifactId=computer-driver -Dversion=0.1.1 -Dpackaging=jar -DpomFile=$TRAVIS_DIR/computer-driver-pom.xml
mvn install:install-file -Dfile=$TRAVIS_DIR/computer-k8s-0.1.1.jar -DgroupId=com.baidu.hugegraph -DartifactId=computer-k8s -Dversion=0.1.1 -Dpackaging=jar -DpomFile=$TRAVIS_DIR/computer-k8s-pom.xml