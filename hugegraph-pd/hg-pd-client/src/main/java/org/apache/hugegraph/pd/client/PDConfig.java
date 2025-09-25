package org.apache.hugegraph.pd.client;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.Base64;
import org.apache.commons.lang3.StringUtils;

import com.baidu.hugegraph.pd.client.interceptor.AuthenticationException;

import lombok.Getter;
import lombok.Setter;

public final class PDConfig {
    //TODO multi-server
    private String serverHost = "localhost:9000";
    private long grpcTimeOut = 60000;   // grpc调用超时时间 10秒
    private boolean enablePDNotify = false; // 是否接收PD异步通知
    private boolean enableCache = false;
    private String authority;
    private String userName = "";
    private static final int GRPC_DEFAULT_MAX_INBOUND_MESSAGE_SIZE = 1024 * 1024 * 1024;
    private static final int GRPC_DEFAULT_MAX_OUTBOUND_MESSAGE_SIZE = 1024 * 1024 * 1024;
    private static int inboundMessageSize = GRPC_DEFAULT_MAX_INBOUND_MESSAGE_SIZE;
    private static int outboundMessageSize = GRPC_DEFAULT_MAX_OUTBOUND_MESSAGE_SIZE;
    @Getter
    @Setter
    private boolean autoGetPdServers = false;

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

    public long getGrpcTimeOut() {
        return grpcTimeOut;
    }

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
        return "PDConfig{ serverHost='" + serverHost + '\'' + '}';
    }

    public PDConfig setAuthority(String userName, String pwd) {
        this.userName = userName;
        String auth = userName + ':' + pwd;
        this.authority = new String(Base64.getEncoder().encode(auth.getBytes(UTF_8)));
        return this;
    }

    public String getUserName() {
        return userName;
    }

    public String getAuthority() {
        if (StringUtils.isEmpty(this.authority)){
            throw new AuthenticationException("invalid basic authentication info");
        }
        return authority;
    }
    public static int getInboundMessageSize() {
        return inboundMessageSize;
    }

    public static void setInboundMessageSize(int inboundMessageSize) {
        PDConfig.inboundMessageSize = inboundMessageSize;
    }

    public static int getOutboundMessageSize() {
        return outboundMessageSize;
    }

    public static void setOutboundMessageSize(int outboundMessageSize) {
        PDConfig.outboundMessageSize = outboundMessageSize;
    }
}
