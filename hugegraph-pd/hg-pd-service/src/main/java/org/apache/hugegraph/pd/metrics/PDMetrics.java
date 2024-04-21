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

package org.apache.hugegraph.pd.metrics;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.pd.service.PDService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public final class PDMetrics {

    public static final String PREFIX = "hg";
    private static final AtomicLong GRAPHS = new AtomicLong(0);
    private MeterRegistry registry;

    @Autowired
    private PDService pdService;

    public synchronized void init(MeterRegistry meterRegistry) {

        if (registry == null) {
            registry = meterRegistry;
            registerMeters();
        }

    }

    private void registerMeters() {
        Gauge.builder(PREFIX + ".up", () -> 1).register(registry);

        Gauge.builder(PREFIX + ".graphs", this::updateGraphs)
             .description("Number of graphs registered in PD")
             .register(registry);

        Gauge.builder(PREFIX + ".stores", this::updateStores)
             .description("Number of stores registered in PD")
             .register(registry);

    }

    private long updateGraphs() {
        long buf = getGraphs();

        if (buf != GRAPHS.get()) {
            GRAPHS.set(buf);
            registerGraphMetrics();
        }
        return buf;
    }

    private long updateStores() {
        return getStores();
    }

    private long getGraphs() {
        return getGraphMetas().size();
    }

    private long getStores() {
        try {
            return this.pdService.getStoreNodeService().getStores(null).size();
        } catch (PDException e) {
            log.error(e.getMessage(), e);
            e.printStackTrace();
        }
        return 0;
    }

    private List<Metapb.Graph> getGraphMetas() {
        try {
            return this.pdService.getPartitionService().getGraphs();
        } catch (PDException e) {
            log.error(e.getMessage(), e);
        }
        return Collections.EMPTY_LIST;
    }

    private void registerGraphMetrics() {
        this.getGraphMetas().forEach(meta -> {
            Gauge.builder(PREFIX + ".partitions", this.pdService.getPartitionService()
                         , e -> e.getPartitions(meta.getGraphName()).size())
                 .description("Number of partitions assigned to a graph")
                 .tag("graph", meta.getGraphName())
                 .register(this.registry);

        });
    }

}
