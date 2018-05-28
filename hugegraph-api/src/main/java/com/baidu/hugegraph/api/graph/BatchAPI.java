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

package com.baidu.hugegraph.api.graph;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.api.API;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.config.ServerOptions;
import com.baidu.hugegraph.metric.MetricsUtil;
import com.baidu.hugegraph.server.RestServer;
import com.baidu.hugegraph.util.Log;
import com.codahale.metrics.Meter;

public class BatchAPI extends API {

    private static final Logger LOG = Log.logger(RestServer.class);

    // NOTE: VertexAPI and EdgeAPI should share a counter
    private static final AtomicInteger batchWriteThreads = new AtomicInteger(0);

    static {
        MetricsUtil.registerGauge(RestServer.class, "batch-write-threads",
                                  () -> batchWriteThreads.intValue());
    }

    private final Meter batchMeter;

    public BatchAPI() {
        this.batchMeter = MetricsUtil.registerMeter(this.getClass(),
                                                    "batch-insert");
    }

    public <R> R commit(HugeConfig config, HugeGraph g, int size,
                        Callable<R> callable) {
        int maxWriteThreads = config.get(ServerOptions.MAX_WRITE_THREADS);
        int writingThreads = batchWriteThreads.incrementAndGet();
        if (writingThreads > maxWriteThreads) {
            batchWriteThreads.decrementAndGet();
            throw new HugeException("The rest server is too busy to write");
        }

        LOG.debug("The batch writing threads is {}", batchWriteThreads);
        try {
            R result = commit(g, callable);
            this.batchMeter.mark(size);
            return result;
        } finally {
            batchWriteThreads.decrementAndGet();
        }
    }
}
