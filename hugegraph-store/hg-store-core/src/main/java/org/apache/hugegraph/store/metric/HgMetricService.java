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

package org.apache.hugegraph.store.metric;

import java.io.File;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.io.FileUtils;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.store.HgStoreEngine;
import org.apache.hugegraph.store.PartitionEngine;
import org.apache.hugegraph.store.util.Lifecycle;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class HgMetricService implements Lifecycle<Void> {

    private final static HgMetricService instance = new HgMetricService();
    private final static AtomicLong bytesWritten = new AtomicLong();
    private final static AtomicLong bytesRead = new AtomicLong();
    private final static AtomicLong keysWritten = new AtomicLong();
    private final static AtomicLong keysRead = new AtomicLong();
    private final static long startTime = Instant.now().getEpochSecond();
    private static long lastQueryTime = 0;
    private final SystemMetricService systemMetricService = new SystemMetricService();
    private HgStoreEngine storeEngine;
    private Map<String, Long> systemMetrics = new HashMap<>();

    private HgMetricService() {
    }

    public static HgMetricService getInstance() {
        return instance;
    }

    @Override
    public boolean init(final Void v) {
        resetMetrics();
        return true;
    }

    @Override
    public void shutdown() {

    }

    public HgMetricService setHgStoreEngine(HgStoreEngine storeEngine) {
        this.storeEngine = storeEngine;
        this.systemMetricService.setStoreEngine(storeEngine);
        return this;
    }

    public Metapb.StoreStats.Builder getMetrics() {
        Metapb.StoreStats.Builder builder = Metapb.StoreStats.newBuilder();
        try {
            getStoreMetrics(builder);
            getRaftMetrics(builder);
            getDiskMetrics(builder);
            getSystemMetrics(builder);
        } catch (Exception e) {
            log.error("HgMetricService getMetrics {}", e);
        }
        return builder;
    }

    private Metapb.StoreStats.Builder getDiskMetrics(Metapb.StoreStats.Builder builder) {
        try {
            long capacity = 0L;
            long available = 0L;
            long used = 0L;
            HashSet<String> fileStoreSet = new HashSet<>();
            for (String dbPath : this.storeEngine.getDataLocations()) {
                FileStore fs = Files.getFileStore(Paths.get(dbPath));
                if (fileStoreSet.contains(fs.name())) {
                    continue;
                }
                fileStoreSet.add(fs.name());
                capacity += fs.getTotalSpace();
                available += fs.getUsableSpace();
                used += FileUtils.sizeOfDirectory(new File(dbPath));
            }
            builder.setCapacity(capacity);
            builder.setAvailable(available);
            builder.setUsedSize(used);
        } catch (Exception e) {
            log.error("Failed to get disk metrics. {}", e.toString());
        }
        return builder;
    }

    private Metapb.StoreStats.Builder getRaftMetrics(Metapb.StoreStats.Builder builder) {
        Map<Integer, PartitionEngine> partitionEngines = this.storeEngine.getPartitionEngines();
        builder.setPartitionCount(partitionEngines.size());
        partitionEngines.forEach((partId, engine) -> {
            builder.addRaftStats(Metapb.RaftStats.newBuilder()
                                                 .setPartitionId(partId)
                                                 .setCommittedIndex(engine.getCommittedIndex())
                                                 .build());
        });
        return builder;
    }

    private Metapb.StoreStats.Builder getStoreMetrics(Metapb.StoreStats.Builder builder) {
        builder.setStoreId(this.storeEngine.getHeartbeatService().getStoreInfo().getId());

        builder.setStartTime((int) startTime);
        this.storeEngine.getPartitionManager().getPartitions().forEach((graphName, partitions) -> {
            partitions.forEach((partId, partition) -> {
                HgStoreMetric.Graph graphMetric =
                        this.storeEngine.getBusinessHandler().getGraphMetric(graphName, partId);
                if ((graphMetric != null) &&
                    (storeEngine.getPartitionManager().getLocalRoleFromShard(partition) != null)) {
                    builder.addGraphStats(Metapb.GraphStats.newBuilder()
                                                           .setGraphName(graphName)
                                                           .setPartitionId(partId)
                                                           .setApproximateKeys(
                                                                   graphMetric.getApproxKeyCount())
                                                           .setApproximateSize(
                                                                   graphMetric.getApproxDataSize())
                                                           .setRole(
                                                                   storeEngine.getPartitionManager()
                                                                              .getLocalRoleFromShard(
                                                                                      partition)
                                                                              .toShardRole())
                                                           .setWorkState(partition.getWorkState())

                                                           .build());
                }

            });
        });

        return builder;
    }

    /**
     * get system metrics each 1 minute
     *
     * @param builder
     * @return
     */
    private Metapb.StoreStats.Builder getSystemMetrics(Metapb.StoreStats.Builder builder) {
        // load each 1 minute
        if (systemMetrics.isEmpty() || System.currentTimeMillis() - lastQueryTime >= 60000) {
            systemMetrics = systemMetricService.getSystemMetrics();
            lastQueryTime = System.currentTimeMillis();
        }

        for (Map.Entry<String, Long> entry : systemMetrics.entrySet()) {
            if (entry.getValue() != null) {
                builder.addSystemMetrics(Metapb.RecordPair.newBuilder()
                                                          .setKey(entry.getKey())
                                                          .setValue(entry.getValue())
                                                          .build());
            }
        }

        return builder;
    }

    private void resetMetrics() {
        bytesWritten.set(0);
        bytesRead.set(0);
        keysWritten.set(0);
        keysRead.set(0);
    }

    public void increaseWriteCount(long keys, long bytes) {
        keysWritten.addAndGet(keys);
        bytesWritten.addAndGet(bytes);
    }

    public void increaseReadCount(long keys, long bytes) {
        keysRead.addAndGet(keys);
        bytesRead.addAndGet(bytes);
    }
}
