package org.apache.hugegraph;

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
    private final String prefix = "graph_creat_tx";

    private KvClient<WatchResponse> client;

    public TxScanner(KvClient<WatchResponse> client) {
    }


    public void scan() {
        try {
            ScanPrefixResponse response = this.client.scanPrefix(prefix);
            for(String key : response.getKvsMap().keySet()) {
                String value = response.getKvsMap().get(key);
                CheckList checkList = JsonUtil.fromJson(value, CheckList.class);
                switch (checkList.getStage()) {
                    case "config": {
                        configContinue(checkList);
                        break;
                    }
                    case "initBackend" : {
                        HugeConfig config = checkList.getConfig();
                        HugeGraph graph = (HugeGraph) GraphFactory.open(config);
                        GlobalMasterInfo globalMasterInfo = checkList.getNodeInfo();
                        graph.serverStarted(globalMasterInfo);
                        // Write config to disk file
                        String confPath = ConfigUtil.writeToFile(checkList.getConfigPath(), graph.name(),
                                                                 (HugeConfig)graph.configuration());
                        break;
                    }
                    case "setServerStarted" : {
                        HugeConfig config = checkList.getConfig();
                        HugeGraph graph = (HugeGraph) GraphFactory.open(config);
                        String confPath = ConfigUtil.writeToFile(checkList.getConfigPath(), graph.name(),
                                                                 (HugeConfig)graph.configuration());
                        break;
                    }
                    case "finish" : {
                        client.delete(prefix + checkList.getName());
                        break;
                    }
                }
            }
        } catch (PDException e) {
            throw new RuntimeException(e);
        }

    }

    private void configContinue(CheckList checkList) {
        HugeConfig config = checkList.getConfig();
        HugeGraph graph = (HugeGraph) GraphFactory.open(config);
        try {
            // Create graph instance
            graph = (HugeGraph) GraphFactory.open(config);
            String configPath = checkList.getConfigPath();
            GlobalMasterInfo globalMasterInfo = checkList.getNodeInfo();
            // Init graph and start it
            graph.create(configPath, globalMasterInfo);
        } catch (Throwable e) {
            throw e;
        }

    }

}
