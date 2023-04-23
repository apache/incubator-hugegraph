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