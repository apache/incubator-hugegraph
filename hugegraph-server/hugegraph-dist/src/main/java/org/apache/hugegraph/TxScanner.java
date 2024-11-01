package org.apache.hugegraph;

import jakarta.ws.rs.core.Context;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.core.GraphManager;
import org.apache.hugegraph.masterelection.GlobalMasterInfo;
import org.apache.hugegraph.pd.client.KvClient;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.grpc.kv.ScanPrefixResponse;
import org.apache.hugegraph.pd.grpc.kv.WatchResponse;
import org.apache.hugegraph.util.ConfigUtil;
import org.apache.hugegraph.util.Events;
import org.apache.hugegraph.util.JsonUtil;
import org.apache.hugegraph.variables.CheckList;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;



public class TxScanner {
    private final String prefix = "graph_creat_tx_";

    @Context
    GraphManager manager;

    private KvClient<WatchResponse> client;

    public TxScanner(KvClient<WatchResponse> client) {
    }


    public void scan() {
        try {
            ScanPrefixResponse response = this.client.scanPrefix(prefix);
            for(String key : response.getKvsMap().keySet()) {
                String value = response.getKvsMap().get(key);
                CheckList checkList = JsonUtil.fromJson(value, CheckList.class);
                HugeGraph graph = manager.createGraphRecover(checkList.getConfig(), checkList.getName());
            }
        } catch (PDException e) {
            throw new RuntimeException(e);
        }
    }
}
