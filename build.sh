#!/usr/bin/env bash

HUGEGRAPH_COMMON_RELEASE_PATH="${PWD}/output/"

export MAVEN_HOME="/home/scmtools/buildkit/maven/apache-maven-3.3.9/"
export JAVA_HOME="/home/scmtools/buildkit/java/jdk1.8.0_25/"
export PATH="$JAVA_HOME/bin:$MAVEN_HOME/bin:$PATH"

mvn clean package -DskipTests

if [ -d $HUGEGRAPH_COMMON_RELEASE_PATH ]; then
	echo "$HUGEGRAPH_COMMON_RELEASE_PATH is already exists."
	exit 1
fi

mkdir -p $HUGEGRAPH_COMMON_RELEASE_PATH || echo "Failed to create directory: $HUGEGRAPH_COMMON_RELEASE_PATH"

cp target/*.jar $HUGEGRAPH_COMMON_RELEASE_PATH
if [ $? -ne 0 ]; then
	echo "Move jar to $HUGEGRAPH_COMMON_RELEASE_PATH failed."
else
	echo "Build Succeed!"
fi

echo "Deploy to baidu maven repository..."

mvn deploy -DskipTests

exit $?