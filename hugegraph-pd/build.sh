#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with this
# work for additional information regarding copyright ownership. The ASF
# licenses this file to You under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
#

export PATH=$MAVEN_3_5_3_BIN:$ORACLEJDK_11_0_7_BIN:$PATH
export JAVA_HOME=$ORACLEJDK_11_0_7_HOME
export MAVEN_HOME=$MAVEN_3_5_3_HOME

# TODO: remove later
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
