#!/bin/bash

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
    JAVA_OPTIONS="-Xms256m -Xmx2048m -javaagent:$LIB/jamm-0.3.0.jar"
fi

# Execute the application and return its exit code
set -x

ARGS="$@"
if [ $# = 0 ] ; then
ARGS="conf/rest-server.properties"
fi
exec $JAVA -Dhugegraph.logdir="HUGEGRAPH_LOGDIR" \
-Dlog4j.configurationFile=conf/rest-server-log4j.xml \
$JAVA_OPTIONS -cp $CP:$CLASSPATH com.baidu.hugegraph.dist.HugeRestServer $ARGS
