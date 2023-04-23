package com.baidu.hugegraph.store.util;

import com.baidu.hugegraph.rocksdb.access.RocksDBSession;
import com.baidu.hugegraph.store.HgStoreEngine;
import com.baidu.hugegraph.store.meta.base.MetaStoreBase;

import java.util.List;

public class PartitionMetaStoreWrapper{

    private static InnerMetaStore store = new InnerMetaStore();


    public void put(int partitionId, byte[] key, byte[] value){
        store.setPartitionId(partitionId);
        store.put(key, value);
    }

    public <T> T get(int partitionId, byte[] key, com.google.protobuf.Parser<T> parser){
        store.setPartitionId(partitionId);
        return store.get(parser, key);
    }

    public byte[] get(int partitionId, byte[] key) {
        store.setPartitionId(partitionId);
        return store.get(key);
    }

    public void delete(int partitionId, byte[] key){
        store.setPartitionId(partitionId);
        store.delete(key);
    }

    public <T> List<T> scan(int partitionId, com.google.protobuf.Parser<T> parser, byte[] prefix) {
        store.setPartitionId(partitionId);
        return store.scan(parser, prefix);
    }

    private static class InnerMetaStore extends MetaStoreBase{

        private int partitionId ;

        private void setPartitionId(int partitionId){
            this.partitionId = partitionId;
        }

        @Override
        protected RocksDBSession getRocksDBSession() {
            return HgStoreEngine.getInstance().getBusinessHandler().getSession(this.partitionId);
        }

        @Override
        protected String getCFName() {
            return "default";
        }
    }
}
