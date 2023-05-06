#!/bin/bash
export PATH=$MAVEN_3_5_3_BIN:$ORACLEJDK_11_0_7_BIN:$PATH
export JAVA_HOME=$ORACLEJDK_11_0_7_HOME
export MAVEN_HOME=$MAVEN_3_5_3_HOME

readonly REPO_URL=http://maven.baidu-int.com/nexus/content/repositories/Baidu_Local_Snapshots

if [ ! -n "$1" ] ;then
    GOAL=package
else
    GOAL=$1
fi

$MAVEN_3_5_3_BIN/mvn -Dmaven.test.skip=true -DaltDeploymentRepository=Baidu_Local_Snapshots::default::${REPO_URL} clean ${GOAL}
echo "mv dist...."
mv dist output
ls output
echo "mv dist done"
echo "show output...."
ls output
echo "show output done"