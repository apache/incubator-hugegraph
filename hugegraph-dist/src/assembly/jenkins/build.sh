#!/bin/bash

# Test
if [ $ACTION = "test" ]; then
    . $SCRIPT_DIR/config.sh
    CONF=`config_${BACKEND}`
    if [ $? -ne 0 ]; then
        echo "Failed to config backend: $CONF"
        exit 1
    fi

    sh $SCRIPT_DIR/test.sh $CONF
# Deploy
elif [ $ACTION = "deploy" ]; then
    sh $SCRIPT_DIR/deploy.sh
# Publish
elif [ $ACTION = "publish" ]; then
    sh $SCRIPT_DIR/publish.sh
fi
