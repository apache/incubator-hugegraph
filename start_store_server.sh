AGILE_PRODUCT_HTTP_URL=https://irepo.baidu-int.com/rest/prod/v3/baidu/starhugegraph/hugegraph-store/nodes/54385742/files
AGILE_COMPILE_BRANCH=3.7.1
DEPLOY_PATH=/home/disk3/store_ci/${AGILE_COMPILE_BRANCH}
PD_PATH=${DEPLOY_PATH}/pd/
STORE_PATH=${DEPLOY_PATH}/store/
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
echo "stop server...."
kill -9 $(jps -mlv|grep ${DEPLOY_PATH}|grep -v grep|awk '{print $1}')
echo "stopped server"
mkdir -p ${PD_PATH}
cd ${PD_PATH}/output/hugegraph-pd
rm -rf pd_data
bin/start-hugegraph-pd.sh
# start store
mkdir -p ${STORE_PATH}
cd ${STORE_PATH}
echo "get store...."
wget -q -O output.tar.gz --no-check-certificate --header "IREPO-TOKEN:1fc56829-86d9-4c81-bb4c-9fabb7ea873d"  ${AGILE_PRODUCT_HTTP_URL}
echo "unzip store tar...."
tar -zxf output.tar.gz
cd output
rm -rf hugegraph-store
find . -name "*.tar.gz" -exec tar -zxvf {} \;
rename
pushd hugegraph-store
echo "changing store application.yml...."
sed -i 's#local os=`uname`#local os=Linux#g' bin/util.sh
sed -i 's/export LD_PRELOAD/#export LD_PRELOAD/' bin/start-hugegraph-store.sh
sed -i 's/host: 127.0.0.1/host: 10.108.17.32/' conf/application.yml
sed -i 's/address: 127.0.0.1:8510/address: 10.108.17.32:8510/' conf/application.yml
bin/start-hugegraph-store.sh
popd
sleep 5
echo "start store end"
