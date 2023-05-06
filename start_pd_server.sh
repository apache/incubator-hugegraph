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

function rename()
{
    cfilelist=$(find  -maxdepth 1 -type d -printf '%f\n' )
    for cfilename in $cfilelist
    do
        if [[ $cfilename =~ SNAPSHOT ]]
        then
           mv $cfilename ${cfilename/-?.?.?-SNAPSHOT/}
        fi
    done
}

wget -q -O output.tar.gz  $AGILE_PRODUCT_HTTP_URL
tar -zxf output.tar.gz
cd output
rm -rf hugegraph-pd
find . -name "*.tar.gz" -exec tar -zxvf {} \;
rename


# start pd
pushd hugegraph-pd
sed -i 's/initial-store-list:.*/initial-store-list: 127.0.0.1:8500\n  initial-store-count: 1/' conf/application.yml
sed -i 's/,127.0.0.1:8611,127.0.0.1:8612//' conf/application.yml
bin/start-hugegraph-pd.sh
popd
jps
sleep 10


# start store
pushd hugegraph-store
sed -i 's#local os=`uname`#local os=Linux#g' bin/util.sh
sed -i 's/export LD_PRELOAD/#export LD_PRELOAD/' bin/start-hugegraph-store.sh
bin/start-hugegraph-store.sh
popd
jps
sleep 5