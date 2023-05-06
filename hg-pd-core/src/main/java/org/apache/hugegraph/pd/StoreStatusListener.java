package org.apache.hugegraph.pd;

import com.baidu.hugegraph.pd.grpc.Metapb;

public interface StoreStatusListener {

    void onStoreStatusChanged(Metapb.Store store, Metapb.StoreState old,
                              Metapb.StoreState status);

    void onGraphChange(Metapb.Graph graph, Metapb.GraphState stateOld,
                               Metapb.GraphState stateNew) ;
    void onStoreRaftChanged(Metapb.Store store);
}
