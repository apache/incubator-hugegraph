AGILE_PRODUCT_HTTP_URL=https://irepo.baidu-int.com/rest/prod/v3/baidu/starhugegraph/hugegraph-pd/nodes/54389398/files
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
# kill -9 $(jps -mlv|grep ${DEPLOY_PATH}/pd/${AGILE_COMPILE_BRANCH}|grep -v grep|awk '{print $1}')
echo "stop server...."
kill -9 $(jps -mlv|grep ${DEPLOY_PATH}|grep -v grep|awk '{print $1}')
echo "stopped server"
mkdir -p ${PD_PATH}
cd ${PD_PATH}
echo "get pd...."
wget -q -O output.tar.gz --no-check-certificate  --header "IREPO-TOKEN:c7404132-b76e-4f48-b77f-478286cbdfb8"   ${AGILE_PRODUCT_HTTP_URL}
tar -zxf output.tar.gz
cd output
rm -rf hugegraph-pd
echo "unzip pd tar...."
find . -name "*.tar.gz" -exec tar -zxvf {} \;
rename
# start pd
echo "changing pd application.yml...."
cd ${PD_PATH}/output/hugegraph-pd
sed -i 's/initial-store-list:.*/initial-store-list: 10.108.17.32:8500\n  initial-store-count: 1/' conf/application.yml
sed -i 's/,127.0.0.1:8611,127.0.0.1:8612//' conf/application.yml
bin/start-hugegraph-pd.sh
sleep 10
echo "start pd end"
mkdir -p ${STORE_PATH}/output
cd ${STORE_PATH}/output
rm -rf hugegraph-store/storage
hugegraph-store/bin/start-hugegraph-store.sh
sleep 10
echo "start store end"