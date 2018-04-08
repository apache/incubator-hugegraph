#!/bin/bash

# The maximum and minium heap memory that service can use
MAX_MEM=$[32*1024]
MIN_MEM=512

function free_memory() {
    FREE
    OS=`uname`
    if [ "$OS" == "Linux" ]; then
        DISTRIBUTOR=`lsb_release -a | grep 'Distributor ID' | awk -F':' '{print $2}' | tr -d "\t"`
        if [ "$DISTRIBUTOR" == "CentOS" ]; then
            FREE=`free -m | grep '\-\/\+' | awk '{print $4}'`
        elif [ "$DISTRIBUTOR" == "Ubuntu" ]; then
            FREE=`free -m | grep 'Mem' | awk '{print $7}'`
        else
            echo "Unsupported Linux Distributor " $DISTRIBUTOR
        fi
    elif [ "$OS" == "Darwin" ]; then
        FREE=`top -l 1 | head -n 10 | grep PhysMem | awk -F',' '{print $2}' \
             | awk -F'M' '{print $1}' | tr -d " "`
    else
        echo "Unsupported operating system " $OS
        exit 1
    fi
    echo $FREE
}

function cal_xmx() {
    # Get machine available memory
    FREE=`free_memory`
    HALF_FREE=$[FREE/2]

    XMX=$MIN_MEM
    if [[ "$FREE" -lt "$MIN_MEM" ]]; then
        exit 1
    elif [[ "$HALF_FREE" -ge "$MAX_MEM" ]]; then
        XMX=$MAX_MEM
    elif [[ "$HALF_FREE" -lt "$MIN_MEM" ]]; then
        XMX=$MIN_MEM
    else
        XMX=$HALF_FREE
    fi
    echo $XMX
}

# ${BASH_SOURCE[0]} is the path to this file
SOURCE="${BASH_SOURCE[0]}"
# Set $BIN to the absolute, symlinkless path to $SOURCE's parent
while [ -h "$SOURCE" ]; do
    BIN="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
    SOURCE="$(readlink "$SOURCE")"
    [[ $SOURCE != /* ]] && SOURCE="$BIN/$SOURCE"
done
BIN="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
# Set $CFG to $BIN/../conf/
cd -P $BIN/../conf
CFG=$(pwd)
# Set $LIB to $BIN/../lib
cd -P $BIN/../lib
LIB=$(pwd)
# Set $LIB to $BIN/../ext
cd -P $BIN/../ext
EXT=$(pwd)
# Initialize classpath to $CFG
CP="$CFG"
# Add the slf4j-log4j12 binding
CP="$CP":$(find -L $LIB -name 'slf4j-log4j12*.jar' | sort | tr '\n' ':')
# Add the jars in $BIN/../lib that start with "hugegraph"
CP="$CP":$(find -L $LIB -name 'hugegraph*.jar' | sort | tr '\n' ':')
# Add the remaining jars in $BIN/../lib.
CP="$CP":$(find -L $LIB -name '*.jar' \
                \! -name 'hugegraph*' \
                \! -name 'slf4j-log4j12*.jar' | sort | tr '\n' ':')
# Add the jars in $BIN/../ext (at any subdirectory depth)
CP="$CP":$(find -L $EXT -name '*.jar' | sort | tr '\n' ':')

# (Cygwin only) Use ; classpath separator and reformat paths for Windows ("C:\foo")
[[ $(uname) = CYGWIN* ]] && CP="$(cygpath -p -w "$CP")"

export CLASSPATH="${CLASSPATH:-}:$CP"

# Change to $BIN's parent
cd $BIN/..

export HUGEGRAPH_LOGDIR="$BIN/../logs"

# Find Java
if [ "$JAVA_HOME" = "" ] ; then
    JAVA="java -server"
else
    JAVA="$JAVA_HOME/bin/java -server"
fi

# Set Java options
if [ "$JAVA_OPTIONS" = "" ] ; then
    XMX=`cal_xmx`
    if [ $? -ne 0 ]; then
        echo "Failed to start HugeGraphServer, requires at least ${MIN_MEM}m free memory" \
        >> $HUGEGRAPH_LOGDIR/hugegraph-server.log
        exit 1
    fi
    JAVA_OPTIONS="-Xms256m -Xmx${XMX}m -javaagent:$LIB/jamm-0.3.0.jar"
fi

# Execute the application and return its exit code
set -x
ARGS="$@"
if [ $# = 0 ] ; then
    ARGS="conf/gremlin-server.yaml conf/rest-server.properties"
fi
exec $JAVA -Dhugegraph.logdir="HUGEGRAPH_LOGDIR" \
-Dlog4j.configurationFile=conf/graph-server-log4j.xml \
$JAVA_OPTIONS -cp $CP:$CLASSPATH com.baidu.hugegraph.dist.HugeGraphServer $ARGS
