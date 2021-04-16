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

import com.alipay.sofa.rpc.common.utils.StringUtils;
import com.baidu.hugegraph.auth.AuthManager;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.config.ServerOptions;
import com.baidu.hugegraph.util.E;

public class RpcClientProviderWithAuth extends RpcClientProvider {

    private final RpcConsumerConfig authConsumerConfig;

    public RpcClientProviderWithAuth(HugeConfig config) {
        super(config);

        String authUrl = config.get(ServerOptions.AUTH_REMOTE_URL);
        this.authConsumerConfig = StringUtils.isNotBlank(authUrl) ?
                                  new RpcConsumerConfig(config, authUrl) : null;
    }

    public AuthManager authManager() {
        E.checkArgument(this.authConsumerConfig != null,
                        "RpcClient is not enabled, please config option '%s'",
                        ServerOptions.AUTH_REMOTE_URL.name());
        return this.authConsumerConfig.serviceProxy(AuthManager.class);
    }
}
