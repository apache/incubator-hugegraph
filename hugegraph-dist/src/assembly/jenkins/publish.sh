#!/bin/bash

OUTPUT="hugegraph-release-*.tar.gz"

echo "Start publish..."

mvn clean package -DskipTests
if [ $? -ne 0 ]; then
    echo "Failed to package."
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
#echo "Publish maven packages..."
#mvn deploy -DskipTests
#if [ $? -ne 0 ]; then
#    echo "Failed to publish."
#    exit 1
#fi

echo "Finish publish."
