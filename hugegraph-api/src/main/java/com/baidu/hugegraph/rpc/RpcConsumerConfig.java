/*
 * Copyright 2017 HugeGraph Authors
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package com.baidu.hugegraph.rpc;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;

import com.alipay.sofa.rpc.bootstrap.ConsumerBootstrap;
import com.alipay.sofa.rpc.client.AbstractCluster;
import com.alipay.sofa.rpc.client.Cluster;
import com.alipay.sofa.rpc.client.ProviderInfo;
import com.alipay.sofa.rpc.config.ConsumerConfig;
import com.alipay.sofa.rpc.core.exception.RpcErrorType;
import com.alipay.sofa.rpc.core.exception.SofaRpcException;
import com.alipay.sofa.rpc.core.request.SofaRequest;
import com.alipay.sofa.rpc.core.response.SofaResponse;
import com.alipay.sofa.rpc.ext.Extension;
import com.alipay.sofa.rpc.ext.ExtensionLoaderFactory;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.config.ServerOptions;
import com.baidu.hugegraph.util.Log;
import com.google.common.collect.Maps;

public class RpcConsumerConfig implements RpcServiceConfig4Client {

    private final HugeConfig conf;
    private final String remoteUrls;
    private final Map<String, ConsumerConfig<?>> configs;

    static {
         ExtensionLoaderFactory.getExtensionLoader(Cluster.class)
                               .loadExtension(FanoutCluster.class);
    }

    public RpcConsumerConfig(HugeConfig conf, String remoteUrls) {
        RpcCommonConfig.initRpcConfigs(conf);
        this.conf = conf;
        this.remoteUrls = remoteUrls;
        this.configs = Maps.newHashMap();
    }

    @Override
    public <T> T serviceProxy(String graph, String interfaceId) {
        ConsumerConfig<T> config = this.consumerConfig(graph, interfaceId);
        return config.refer();
    }

    @Override
    public <T> T serviceProxy(String interfaceId) {
        ConsumerConfig<T> config = this.consumerConfig(null, interfaceId);
        return config.refer();
    }

    private <T> ConsumerConfig<T> consumerConfig(String graph,
                                                 String interfaceId) {
        String serviceId;
        if (graph != null) {
            serviceId = interfaceId + ":" + graph;
        } else {
            serviceId = interfaceId;
        }

        @SuppressWarnings("unchecked")
        ConsumerConfig<T> consumerConfig = (ConsumerConfig<T>)
                                           this.configs.get(serviceId);
        if (consumerConfig != null) {
            return consumerConfig;
        }

        assert consumerConfig == null;
        consumerConfig = new ConsumerConfig<>();

        HugeConfig conf = this.conf;
        String protocol = conf.get(ServerOptions.RPC_PROTOCOL);
        int timeout = conf.get(ServerOptions.RPC_CLIENT_READ_TIMEOUT) * 1000;
        int connectTimeout = conf.get(ServerOptions
                                      .RPC_CLIENT_CONNECT_TIMEOUT) * 1000;
        int reconnectPeriod = conf.get(ServerOptions
                                       .RPC_CLIENT_RECONNECT_PERIOD) * 1000;
        int retries = conf.get(ServerOptions.RPC_CLIENT_RETRIES);
        String loadBalancer = conf.get(ServerOptions.RPC_CLIENT_LOAD_BALANCER);

        if (graph != null) {
            consumerConfig.setId(serviceId).setUniqueId(graph);
            // Default is FailoverCluster, set to FanoutCluster to broadcast
            consumerConfig.setCluster("fanout");
        }
        consumerConfig.setInterfaceId(interfaceId)
                      .setProtocol(protocol)
                      .setDirectUrl(this.remoteUrls)
                      .setTimeout(timeout)
                      .setConnectTimeout(connectTimeout)
                      .setReconnectPeriod(reconnectPeriod)
                      .setRetries(retries)
                      .setLoadBalancer(loadBalancer);

        this.configs.put(serviceId, consumerConfig);
        return consumerConfig;
    }

    @Extension("fanout")
    private static class FanoutCluster extends AbstractCluster {

        private static final Logger LOG = Log.logger(FanoutCluster.class);

        public FanoutCluster(ConsumerBootstrap<?> consumerBootstrap) {
            super(consumerBootstrap);
        }

        @Override
        protected SofaResponse doInvoke(SofaRequest request)
                                        throws SofaRpcException {
            List<ProviderInfo> providers = this.getRouterChain()
                                               .route(request, null);
            List<SofaResponse> responses = new ArrayList<>(providers.size());
            List<SofaRpcException> excepts = new ArrayList<>(providers.size());

            for (ProviderInfo provider : providers) {
                try {
                    SofaResponse response = this.doInvoke(request, provider);
                    responses.add(response);
                } catch (SofaRpcException e) {
                    excepts.add(e);
                    LOG.warn("{}.(error {})", e.getMessage(), e.getErrorType());
                }
            }

            if (responses.size() > 0) {
                // Just choose one
                return responses.get(0);
            } else if (excepts.size() > 0) {
                throw excepts.get(0);
            } else {
                assert providers.isEmpty();
                String method = methodName(request);
                throw new SofaRpcException(RpcErrorType.CLIENT_ROUTER,
                                           "No service provider for " + method);
            }
        }

        private SofaResponse doInvoke(SofaRequest request,
                                      ProviderInfo providerInfo) {
            try {
                SofaResponse response = this.filterChain(providerInfo, request);
                if (response != null) {
                    return response;
                }
                String method = methodName(request);
                throw new SofaRpcException(RpcErrorType.CLIENT_UNDECLARED_ERROR,
                          "Failed to call " + method + " on remote server " +
                          providerInfo + ", return null response");
            } catch (SofaRpcException e) {
                throw e;
            } catch (Exception e) {
                String method = methodName(request);
                throw new SofaRpcException(RpcErrorType.CLIENT_UNDECLARED_ERROR,
                          "Failed to call " + method + " on remote server " +
                          providerInfo + ", cause by exception: " + e);
            }
        }

        private static String methodName(SofaRequest request) {
            String method = request.getInterfaceName() + "." +
                            request.getMethodName() + "()";
            return method;
        }
    }
}
