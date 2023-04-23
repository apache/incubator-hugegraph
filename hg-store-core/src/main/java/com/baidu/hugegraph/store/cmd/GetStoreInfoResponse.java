package com.baidu.hugegraph.store.cmd;

import com.baidu.hugegraph.pd.grpc.Metapb;
import com.baidu.hugegraph.store.meta.Store;
import com.google.protobuf.InvalidProtocolBufferException;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GetStoreInfoResponse extends HgCmdBase.BaseResponse{
    private byte[] store;

    public void setStore(Store store) {
        this.store = store.getProtoObj().toByteArray();
    }

    public Store getStore(){
        try {
            return new Store(Metapb.Store.parseFrom(this.store));
        } catch (InvalidProtocolBufferException e) {
            log.error("GetStoreResponse parse exception {}", e);
        }
        return null;
    }
}
