package org.apache.hugegraph.pd.pulse.impl;

import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.pd.grpc.pulse.StoreNodeEventType;
import org.apache.hugegraph.pd.listener.StoreStatusListener;
import org.apache.hugegraph.pd.pulse.PDPulseSubjects;

public class StoreStatusListenerImpl implements StoreStatusListener {
    @Override
    public void onStoreStatusChanged(Metapb.Store store,
                                     Metapb.StoreState old,
                                     Metapb.StoreState status) {
        StoreNodeEventType type = StoreNodeEventType.STORE_NODE_EVENT_TYPE_UNKNOWN;
        if (status == Metapb.StoreState.Up) {
            type = StoreNodeEventType.STORE_NODE_EVENT_TYPE_NODE_ONLINE;
        } else if (status == Metapb.StoreState.Offline) {
            type = StoreNodeEventType.STORE_NODE_EVENT_TYPE_NODE_OFFLINE;
        }
        PDPulseSubjects.notifyNodeChange(type, "", store.getId());
    }

    @Override
    public void onGraphChange(Metapb.Graph graph,
                              Metapb.GraphState stateOld,
                              Metapb.GraphState stateNew) {
//                PulseGraphResponse wgr = PulseGraphResponse.newBuilder()
//                                                           .setGraph(graph)
//                                                           .build();
//                PulseResponse.Builder wr = PulseResponse.newBuilder()
//                                                        .setGraphResponse(wgr);
//                PDPulseSubjects.notifyChange(WatchType.WATCH_TYPE_GRAPH_CHANGE,
//                                            wr);
        PDPulseSubjects.notifyGraphChange(graph);
    }

    @Override
    public void onStoreRaftChanged(Metapb.Store store) {
        PDPulseSubjects.notifyNodeChange(StoreNodeEventType.STORE_NODE_EVENT_TYPE_NODE_RAFT_CHANGE, "",
                                         store.getId());
    }
}
