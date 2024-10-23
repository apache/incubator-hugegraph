package org.apache.hugegraph.variables;

import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.masterelection.GlobalMasterInfo;

import java.io.Serializable;

public class CheckList implements Serializable {

    private String name;
    private String configText;
    
    private HugeConfig config;

    private boolean initBackended;

    private boolean serverStarted;

    private String stage;

    private String configPath;

    private GlobalMasterInfo nodeInfo;

    boolean toCheck;
    String context;
    private boolean isBuild;

    public void setBuild(boolean build) {
        isBuild = build;
    }

    public CheckList(String name, String context) {
        this.name = name;
        this.context = context;
    }

    public HugeConfig getConfig() {
        return config;
    }

    public void setConfig(HugeConfig config) {
        this.config = config;
    }

    public boolean isInitBackended() {
        return initBackended;
    }

    public void setInitBackended(boolean initBackended) {
        this.initBackended = initBackended;
    }

    public boolean isServerStarted() {
        return serverStarted;
    }

    public void setServerStarted(boolean serverStarted) {
        this.serverStarted = serverStarted;
    }

    public String getStage() {
        return stage;
    }

    public void setStage(String stage) {
        this.stage = stage;
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

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
