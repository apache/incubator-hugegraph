#!/usr/bin/env bash

ACTION=${ACTION}
BUILD_ID=${AGILE_COMPILE_BUILD_ID}
BRANCH_REF=${AGILE_COMPILE_BRANCH_REF}

USER="liunanke"
REPO="hugegraph"
REPO_URL="icode.baidu.com:8235/baidu/xbu-data/$REPO"
OUTPUT="hugegraph-release-*.tar.gz"
RELEASE_SERVER="yq01-sw-hdsserver16.yq01.baidu.com 8222"
RELEASE_SERVER_USER=${FTP_USER}

CONF="hugegraph-test/src/main/resources/hugegraph.properties"
STORE="hugegraph_$BUILD_ID"
HOST="yq01-sw-hugegraph01.yq01.baidu.com"
PORT="8052"

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

# Fetch code from repo if necessary
if [ -n "$BRANCH_REF" ]; then
    echo "Fetch code from repo: ${BRANCH_REF}..."
    git fetch ssh://$USER@$REPO_URL ${BRANCH_REF} && git checkout FETCH_HEAD
    if [ $? -ne 0 ]; then
        echo "Failed to fetch code."
        exit 1
    fi
fi

# Set options into conf of core-test
echo "Set conf options: $STORE $HOST $PORT"
sed -i "s/store=.*/store=$STORE/" $CONF
sed -i "s/cassandra\.host=.*/cassandra\.host=$HOST/" $CONF
sed -i "s/cassandra\.port=.*/cassandra\.port=$PORT/" $CONF

# Build and test
if [ $ACTION = "build" ]; then

    echo "Start build..."

    mvn clean package
    if [ $? -ne 0 ]; then
        echo "Failed to build or test."
        exit 1
    fi

    echo "Finish build."

# Deploy or publish
elif [ $ACTION = "deploy" ]; then

    echo "Start deploy..."

    mvn clean package
    if [ $? -ne 0 ]; then
        echo "Failed to build or test."
        exit 1
    fi

    echo "Upload $OUTPUT to release server $RELEASE_SERVER..."
    ftp -n <<- FTP_EOF
        open $RELEASE_SERVER
        user $RELEASE_SERVER_USER
        cd /download/$REPO
        bin
        put $OUTPUT
        bye
FTP_EOF

    if [ $? -ne 0 ]; then
        echo "Failed to upload."
        exit 1
    fi

    # Use maven-deploy plugin instead if 401 error
    echo "Deploy maven packages..."
    mvn deploy -DskipTests
    if [ $? -ne 0 ]; then
        echo "Failed to deploy."
        exit 1
    fi

    echo "Finish deploy."

fi
