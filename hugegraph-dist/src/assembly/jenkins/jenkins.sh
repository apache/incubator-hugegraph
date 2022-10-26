#!/bin/bash
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
# Backends contains [memory, rocksdb, cassandra, scylladb, mysql]
export BACKEND=memory
# The jenkins script store path
export SCRIPT_DIR="hugegraph-dist/src/assembly/jenkins"

# The jenkins job integrated behavior: [test, deploy, publish]
export ACTION=${ACTION}
# How to trigger the jenkins job: [before-merge, after-merge]
export TRIGGER=${TRIGGER}
# The test cases run mode: [serial, parallel]
export RUNMODE=${RUNMODE}

export BUILD_ID=${AGILE_COMPILE_BUILD_ID}
export BRANCH=${AGILE_COMPILE_BRANCH}
export BRANCH_REF=${AGILE_COMPILE_BRANCH_REF}

# The user who clone code repository
export USER=${GIT_USER}
# The name of code repository
export REPO=${GIT_REPO}
# The url of the code repository for cloning
export REPO_URL=${GIT_REPO_URL}

# We will upload the compiled compressed package to a server using FTP and
# generate a download link for the user to download, this server called
# release server
export RELEASE_SERVER=${RELEASE_SERVER}
# The FTP user
export RELEASE_SERVER_USER=${FTP_USER}

# cassandra config
export CASSANDRA_HOST=${CASSANDRA_HOST}
export CASSANDRA_PORT=${CASSANDRA_PORT}

# scylladb config
export SCYLLADB_HOST=${SCYLLADB_HOST}
export SCYLLADB_PORT=${SCYLLADB_PORT}

# mysql config
export MYSQL_JDBC_URL=${MYSQL_JDBC_URL}
export MYSQL_JDBC_USERNAME=${MYSQL_JDBC_USERNAME}

# Clone code from repo if necessary
if [ ! -d $REPO ]; then
    echo "Clone code from repo..."
    git clone ssh://$USER@$REPO_URL
    if [ $? -ne 0 ]; then
        echo "Failed to clone code."
        exit 1
    fi
fi

# Change dir into local repo
cd $REPO
if [ $? -ne 0 ]; then
    echo "Failed to cd $REPO."
    exit 1
fi

if [ -n "$BRANCH_REF" ]; then
    # Fetch code from repo if necessary
    echo "Fetch code from repo: ${BRANCH_REF}..."
    git checkout . && git checkout $BRANCH
    git fetch ssh://$USER@$REPO_URL ${BRANCH_REF} && git checkout FETCH_HEAD
    if [ $? -ne 0 ]; then
        echo "Failed to fetch code."
        exit 1
    fi
else
    # Pull or checkout release branch
    git checkout .
    git rev-parse --verify $BRANCH >/dev/null 2>&1
    if [ $? -eq 0 ]; then
        git checkout $BRANCH && git pull
    else
        git fetch origin && git checkout -b $BRANCH origin/$BRANCH
    fi

    if [ $? -ne 0 ]; then
        echo "Failed to pull code."
        exit 1
    fi
fi

sh $SCRIPT_DIR/build.sh
