package org.apache.hugegraph.pd.client;

public final class PDConfig {
    //TODO multi-server
    private String serverHost = "localhost:9000";
    private long grpcTimeOut = 60000;   // grpc调用超时时间 10秒

    // 是否接收PD异步通知
    private boolean enablePDNotify = false;

    private boolean enableCache = false;

    private PDConfig() {
    }

    public static PDConfig of() {
        return new PDConfig();
    }

    public static PDConfig of(String serverHost) {
        PDConfig config = new PDConfig();
        config.serverHost = serverHost;
        return config;
    }

    public static PDConfig of(String serverHost, long timeOut) {
        PDConfig config = new PDConfig();
        config.serverHost = serverHost;
        config.grpcTimeOut = timeOut;
        return config;
    }
    public String getServerHost() {
        return serverHost;
    }

    public long getGrpcTimeOut(){ return grpcTimeOut; }

    @Deprecated
    public PDConfig setEnablePDNotify(boolean enablePDNotify) {
        this.enablePDNotify = enablePDNotify;

        // TODO 临时代码，hugegraph修改完后删除
        this.enableCache = enablePDNotify;
        return this;
    }

    public boolean isEnableCache() {
        return enableCache;
    }

    public PDConfig setEnableCache(boolean enableCache) {
        this.enableCache = enableCache;
        return this;
    }

    @Override
    public String toString() {
        return "PDConfig{" +
                "serverHost='" + serverHost + '\'' +
                '}';
    }
}
