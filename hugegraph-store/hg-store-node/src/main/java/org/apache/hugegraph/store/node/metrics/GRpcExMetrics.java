/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hugegraph.store.node.metrics;

import java.util.concurrent.ThreadPoolExecutor;

import org.apache.hugegraph.store.node.grpc.GRpcServerConfig;
import org.apache.hugegraph.store.node.util.HgExecutorUtil;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;

/**
 * 2022/3/8
 */
public class GRpcExMetrics {

    public final static String PREFIX = "grpc";
    private final static ExecutorWrapper wrapper = new ExecutorWrapper();
    private static MeterRegistry registry;

    private GRpcExMetrics() {
    }

    public synchronized static void init(MeterRegistry meterRegistry) {
        if (registry == null) {
            registry = meterRegistry;
            registerMeters();
        }
    }

    private static void registerMeters() {
        registerExecutor();

    }

    private static void registerExecutor() {

        Gauge.builder(PREFIX + ".executor.pool.size", wrapper, (e) -> e.getPoolSize())
             .description("The current number of threads in the pool.")
             .register(registry);

        Gauge.builder(PREFIX + ".executor.core.pool.size", wrapper, (e) -> e.getCorePoolSize())
             .description(
                     "The largest number of threads that have ever simultaneously been in the " +
                     "pool.")
             .register(registry);

        Gauge.builder(PREFIX + ".executor.active.count", wrapper, (e) -> e.getActiveCount())
             .description("The approximate number of threads that are actively executing tasks.")
             .register(registry);
    }

    private static class ExecutorWrapper {

        ThreadPoolExecutor pool;

        void init() {
            if (this.pool == null) {
                pool = HgExecutorUtil.getThreadPoolExecutor(GRpcServerConfig.EXECUTOR_NAME);
            }
        }

        double getPoolSize() {
            init();
            return this.pool == null ? 0d : this.pool.getPoolSize();
        }

        int getCorePoolSize() {
            init();
            return this.pool == null ? 0 : this.pool.getCorePoolSize();
        }

        int getActiveCount() {
            init();
            return this.pool == null ? 0 : this.pool.getActiveCount();
        }

    }

}
