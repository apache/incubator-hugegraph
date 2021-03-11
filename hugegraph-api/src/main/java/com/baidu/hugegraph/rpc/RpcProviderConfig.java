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

import java.util.Map;

import com.alipay.sofa.rpc.config.ProviderConfig;
import com.google.common.collect.Maps;

public class RpcProviderConfig implements RpcServiceConfig4Server {

    private final Map<String, ProviderConfig<?>> configs = Maps.newHashMap();

    @Override
    public <T, E extends T> void addService(Class<T> clazz, E serviceImpl) {
        this.addService(null, clazz.getName(), serviceImpl);
    }

    @Override
    public <T, E extends T> void addService(String graph, Class<T> clazz,
                                            E serviceImpl) {
        this.addService(graph, clazz.getName(), serviceImpl);
    }

    private <T, E extends T> void addService(String graph,
                                             String interfaceId,
                                             E serviceImpl) {
        ProviderConfig<T> providerConfig = new ProviderConfig<>();
        String serviceId;
        if (graph != null) {
            serviceId = interfaceId + ":" + graph;
            providerConfig.setId(serviceId).setUniqueId(graph);
        } else {
            serviceId = interfaceId;
        }
        providerConfig.setInterfaceId(interfaceId)
                      .setRef(serviceImpl);
        this.configs.put(serviceId, providerConfig);
    }

    public Map<String, ProviderConfig<?>> configs() {
        return this.configs;
    }
}
