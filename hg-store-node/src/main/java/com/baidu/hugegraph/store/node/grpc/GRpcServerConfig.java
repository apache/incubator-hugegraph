package com.baidu.hugegraph.store.node.grpc;

import org.lognet.springboot.grpc.GRpcServerBuilderConfigurer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.baidu.hugegraph.store.node.AppConfig;
import com.baidu.hugegraph.store.node.util.HgExecutorUtil;

import io.grpc.ServerBuilder;

/**
 * @author lynn.bond@hotmail.com on 2022/3/4
 */
@Component
public class GRpcServerConfig extends GRpcServerBuilderConfigurer {
    public final static String EXECUTOR_NAME = "hg-grpc";
    @Autowired
    private AppConfig appConfig;

    @Override
    public void configure(ServerBuilder<?> serverBuilder) {
        AppConfig.ThreadPoolGrpc grpc = appConfig.getThreadPoolGrpc();
        serverBuilder.executor(HgExecutorUtil.createExecutor(EXECUTOR_NAME, grpc.getCore(), grpc.getMax(),
                                                             grpc.getQueue())
        );
    }

}
