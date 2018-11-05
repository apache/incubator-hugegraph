#!/bin/bash

# This script is used to install hugegraph as a system service
# Usage: install.sh port

function abs_path() {
    SOURCE="${BASH_SOURCE[0]}"
    while [ -h "$SOURCE" ]; do
        DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
        SOURCE="$(readlink "$SOURCE")"
        [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE"
    done
    echo "$( cd -P "$( dirname "$SOURCE" )" && pwd )"
}

# Variables
BIN=`abs_path`
TOP="$(cd $BIN/../ && pwd)"

# Command "service" related
SCRIPT_NAME=hugegraph
SRC_SCRIPT=$BIN/$SCRIPT_NAME
INIT_DIR=/etc/init.d

# Command "systemctl" related
SERVICE=hugegraph.service
SERVICE_FILE=$BIN/$SERVICE
START_SCRIPT=start-hugegraph.sh
STOP_SCRIPT=stop-hugegraph.sh
SYSTEMD_DIR=/etc/systemd/system

install_to_service() {
    # Set HugeGraphServer port if provided
    read -t 30 -p "Please input HugeGraphServer port:" SERVER_PORT
    sed -i "s/SERVER_PORT=/SERVER_PORT=${SERVER_PORT}/g" $SRC_SCRIPT

    # Set INSTALL PATH
    sed -i "s?INSTALL_DIR=?INSTALL_DIR=${TOP}?g" $SRC_SCRIPT

    # Install
    sudo cp -fp $SRC_SCRIPT $INIT_DIR
    sudo chmod +x $INIT_DIR/$SCRIPT_NAME
}

install_to_systemd() {
    # Set working directory
    sed -i "s?ExecStart=?ExecStart=${BIN}/${START_SCRIPT}?g" $SERVICE_FILE
    sed -i "s?ExecStop=?ExecStop=${BIN}/${STOP_SCRIPT}?g" $SERVICE_FILE
    # Install
    sudo cp -fp $SERVICE_FILE $SYSTEMD_DIR
    sudo chmod +x $SYSTEMD_DIR/$SERVICE
    # Make update work
    sudo systemctl daemon-reload
    # Start on boot
    sudo systemctl enable $SERVICE
}

test_command() {
    command -v $1 >/dev/null 2>&1
    return $?
}

# Install HugeGraph as service
if test_command systemctl; then
    install_to_systemd
elif test_command service; then
    install_to_service
else
    echo "System must support either systemctl or service, but none is supported now."
    exit 1
fi

# Init store
$BIN/init-store.sh
