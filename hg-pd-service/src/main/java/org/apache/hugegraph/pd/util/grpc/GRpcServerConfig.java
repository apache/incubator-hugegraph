package org.apache.hugegraph.pd.util.grpc;

import org.lognet.springboot.grpc.GRpcServerBuilderConfigurer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.apache.hugegraph.pd.config.PDConfig;
import org.apache.hugegraph.pd.consts.PoolNames;
import org.apache.hugegraph.pd.util.HgExecutorUtil;

import io.grpc.ServerBuilder;

@Component
public class GRpcServerConfig extends GRpcServerBuilderConfigurer {

    public static final int MAX_INBOUND_MESSAGE_SIZE = 1024 * 1024 * 1024;
    @Autowired
    private PDConfig pdConfig;

    @Override
    public void configure(ServerBuilder<?> serverBuilder) {
        PDConfig.ThreadPoolGrpc poolGrpc = pdConfig.getThreadPoolGrpc();
        serverBuilder.executor(
                HgExecutorUtil.createExecutor(PoolNames.GRPC, poolGrpc.getCore(), poolGrpc.getMax(),
                                              poolGrpc.getQueue()));
        serverBuilder.maxInboundMessageSize(MAX_INBOUND_MESSAGE_SIZE);
    }

}
