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

import java.util.List;
import java.util.Set;

import javax.inject.Singleton;
import javax.ws.rs.ServiceUnavailableException;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.PreMatching;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.PathSegment;
import javax.ws.rs.ext.Provider;

import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.config.ServerOptions;
import com.baidu.hugegraph.define.WorkLoad;
import com.baidu.hugegraph.license.LicenseVerifier;
import com.baidu.hugegraph.util.Bytes;
import com.baidu.hugegraph.util.E;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.RateLimiter;

@Provider
@Singleton
@PreMatching
public class LoadDetectFilter implements ContainerRequestFilter {

    private static final Set<String> WHITE_API_LIST = ImmutableSet.of(
            "",
            "apis",
            "metrics",
            "versions"
    );

    // Call gc every 30+ seconds if memory is low and request frequently
    private static final RateLimiter GC_RATE_LIMITER =
                         RateLimiter.create(1.0 / 30);

    @Context
    private javax.inject.Provider<HugeConfig> configProvider;
    @Context
    private javax.inject.Provider<WorkLoad> loadProvider;

    @Override
    public void filter(ContainerRequestContext context) {
        LicenseVerifier.instance().verify();

        if (LoadDetectFilter.isWhiteAPI(context)) {
            return;
        }

        HugeConfig config = this.configProvider.get();

        int maxWorkerThreads = config.get(ServerOptions.MAX_WORKER_THREADS);
        WorkLoad load = this.loadProvider.get();
        // There will be a thread doesn't work, dedicated to statistics
        if (load.incrementAndGet() >= maxWorkerThreads) {
            throw new ServiceUnavailableException(String.format(
                      "The server is too busy to process the request, " +
                      "you can config %s to adjust it or try again later",
                      ServerOptions.MAX_WORKER_THREADS.name()));
        }

        long minFreeMemory = config.get(ServerOptions.MIN_FREE_MEMORY);
        long allocatedMem = Runtime.getRuntime().totalMemory() -
                            Runtime.getRuntime().freeMemory();
        long presumableFreeMem = (Runtime.getRuntime().maxMemory() -
                                  allocatedMem) / Bytes.MB;
        if (presumableFreeMem < minFreeMemory) {
            gcIfNeeded();
            throw new ServiceUnavailableException(String.format(
                      "The server available memory %s(MB) is below than " +
                      "threshold %s(MB) and can't process the request, " +
                      "you can config %s to adjust it or try again later",
                      presumableFreeMem, minFreeMemory,
                      ServerOptions.MIN_FREE_MEMORY.name()));
        }
    }

    public static boolean isWhiteAPI(ContainerRequestContext context) {
        List<PathSegment> segments = context.getUriInfo().getPathSegments();
        E.checkArgument(segments.size() > 0, "Invalid request uri '%s'",
                        context.getUriInfo().getPath());
        String rootPath = segments.get(0).getPath();
        return WHITE_API_LIST.contains(rootPath);
    }

    private static void gcIfNeeded() {
        if (GC_RATE_LIMITER.tryAcquire(1)) {
            System.gc();
        }
    }
}
