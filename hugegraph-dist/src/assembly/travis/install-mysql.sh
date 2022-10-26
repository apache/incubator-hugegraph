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
set -ev

TRAVIS_DIR=$(dirname "$0")
#MYSQL_DOWNLOAD_ADDRESS="http://dev.MySQL.com/get/Downloads"
#MYSQL_VERSION="MySQL-5.7"
#MYSQL_PACKAGE="mysql-5.7.11-Linux-glibc2.5-x86_64"
#MYSQL_TAR="${MYSQL_PACKAGE}.tar.gz"

if [ -d /var/lib/mysql ]; then
    # Reference from https://github.com/mozilla/treeherder/blob/master/bin/travis-setup.sh
    # Using tmpfs for the MySQL data directory reduces travis test runtime by 6x
    sudo mkdir /mnt/ramdisk
    sudo mount -t tmpfs -o size=1024m tmpfs /mnt/ramdisk
    sudo mv /var/lib/mysql /mnt/ramdisk
    sudo ln -s /mnt/ramdisk/mysql /var/lib/mysql
    sudo cp "$TRAVIS_DIR"/mysql.cnf /etc/mysql/conf.d/mysql.cnf
    sudo service mysql restart
else
    echo "Please install mysql firstly."
    exit 1
fi
