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

package com.baidu.hugegraph.api.filter;

import java.io.IOException;

import javax.inject.Singleton;
import javax.ws.rs.ServiceUnavailableException;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.PreMatching;
import javax.ws.rs.core.Context;
import javax.ws.rs.ext.Provider;

import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.config.ServerOptions;
import com.baidu.hugegraph.core.WorkLoad;
import com.baidu.hugegraph.util.Bytes;

@Provider
@Singleton
@PreMatching
public class LoadDetectFilter implements ContainerRequestFilter {

    @Context
    private javax.inject.Provider<HugeConfig> configProvider;
    @Context
    private javax.inject.Provider<WorkLoad> loadProvider;

    @Override
    public void filter(ContainerRequestContext context) throws IOException {
        HugeConfig config = this.configProvider.get();
        long minFreeMemory = config.get(ServerOptions.MIN_FREE_MEMORY);
        long available = Runtime.getRuntime().freeMemory() / Bytes.MB;
        if (available < minFreeMemory) {
            throw new ServiceUnavailableException(String.format(
                      "The server available memory %s(MB) is below than " +
                      "threshold %s(MB) and can't process the request, " +
                      "you can config %s to adjust it or try again later",
                      available, minFreeMemory,
                      ServerOptions.MIN_FREE_MEMORY.name()));
        }

        int maxWorkerThreads = config.get(ServerOptions.MAX_WORKER_THREADS);
        WorkLoad load = this.loadProvider.get();
        // There will be a thread doesn't work, dedicated to statistics
        if (load.incrementAndGet() >= maxWorkerThreads) {
            throw new ServiceUnavailableException(String.format(
                      "The server is too busy to process the request, " +
                      "you can config %s to adjust it or try again later",
                      ServerOptions.MAX_WORKER_THREADS.name()));
        }
    }
}
