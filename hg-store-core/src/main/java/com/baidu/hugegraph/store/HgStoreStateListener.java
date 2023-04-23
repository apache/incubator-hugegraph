package com.baidu.hugegraph.store;

import com.baidu.hugegraph.pd.grpc.Metapb;
import com.baidu.hugegraph.store.meta.Store;

public interface HgStoreStateListener {
    void stateChanged(Store store, Metapb.StoreState oldState, Metapb.StoreState newState);
}
