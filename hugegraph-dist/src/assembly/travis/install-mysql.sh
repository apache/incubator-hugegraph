#!/bin/bash

set -ev

TRAVIS_DIR=`dirname $0`
MYSQL_DOWNLOAD_ADDRESS="http://dev.MySQL.com/get/Downloads"
MYSQL_VERSION="MySQL-5.7"
MYSQL_PACKAGE="mysql-5.7.11-Linux-glibc2.5-x86_64"
MYSQL_TAR="${MYSQL_PACKAGE}.tar.gz"

echo '-----> Configuring MySQL'
# Using tmpfs for the MySQL data directory reduces pytest runtime by 30%.
sudo mkdir /mnt/ramdisk
sudo mount -t tmpfs -o size=1024m tmpfs /mnt/ramdisk
sudo mv /var/lib/mysql /mnt/ramdisk
sudo ln -s /mnt/ramdisk/mysql /var/lib/mysql
sudo cp $TRAVIS_DIR/mysql.cnf /etc/mysql/conf.d/treeherder.cnf
sudo service mysql restart
