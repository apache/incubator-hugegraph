package org.apache.hugegraph.variables;

import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.masterelection.GlobalMasterInfo;
import org.apache.hugegraph.pd.client.KvClient;
import org.apache.hugegraph.pd.client.PDConfig;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.grpc.kv.WatchResponse;
import org.apache.hugegraph.util.JsonUtil;
import org.junit.Before;

import java.io.Serializable;

public class CheckList implements Serializable {

    private String name;

    private HugeConfig config;

    private boolean taskSchedulerInit;

    private boolean serverInfoManagerInit;

    private boolean authManagerInit;

    private boolean initServerInfo;

    private String configPath;

    private GlobalMasterInfo nodeInfo;

    private KvClient<WatchResponse> client;


    private final String preFix = "graph_creat_tx_";

    @Before
    public void setUp() {
        this.client = new KvClient<>(PDConfig.of("localhost:8686"));
    }

    public CheckList(String name, HugeConfig config) {
        this.name = name;
        this.config = config;
    }

    public String getName() {
        return name;
    }

    public boolean isTaskSchedulerInit() {
        return taskSchedulerInit;
    }

    public void setTaskSchedulerInit() {
        this.taskSchedulerInit = true;
        String json = JsonUtil.toJson(this);
        tryPut(json);
    }

    public boolean isServerInfoManagerInit() {
        return serverInfoManagerInit;
    }

    public void setServerInfoManagerInit() {
        this.serverInfoManagerInit = true;
        String json = JsonUtil.toJson(this);
        tryPut(json);
    }

    public boolean isAuthManagerInit() {
        return authManagerInit;
    }

    public void setAuthManagerInit() {
        this.authManagerInit = true;
        String json = JsonUtil.toJson(this);
        tryPut(json);
    }

    public boolean isInitServerInfo() {
        return initServerInfo;
    }

    public void setInitServerInfo() {
        this.initServerInfo = true;
        String json = JsonUtil.toJson(this);
        tryPut(json);
    }

    private void tryPut(String json) {
        try {
            client.put(preFix + name, json);
        } catch (PDException e) {
            throw new RuntimeException(e);
        }
    }

    public String getConfigPath() {
        return configPath;
    }

    public void setConfigPath(String configPath) {
        this.configPath = configPath;
    }

    public GlobalMasterInfo getNodeInfo() {
        return nodeInfo;
    }

    public void setNodeInfo(GlobalMasterInfo nodeInfo) {
        this.nodeInfo = nodeInfo;
    }

    public HugeConfig getConfig() {
        return config;
    }
}
