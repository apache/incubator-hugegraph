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

package org.apache.hugegraph.store.cli.scan;

import java.util.Arrays;
import java.util.List;

import org.apache.hugegraph.pd.client.PDClient;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.store.HgKvEntry;
import org.apache.hugegraph.store.HgKvIterator;
import org.apache.hugegraph.store.HgKvStore;
import org.apache.hugegraph.store.HgScanQuery;
import org.apache.hugegraph.store.HgSessionManager;
import org.apache.hugegraph.store.HgStoreClient;
import org.apache.hugegraph.store.HgStoreSession;
import org.apache.hugegraph.store.cli.util.HgCliUtil;
import org.apache.hugegraph.store.cli.util.HgMetricX;
import org.apache.hugegraph.store.client.grpc.KvCloseableIterator;
import org.apache.hugegraph.store.client.util.HgStoreClientConfig;
import org.apache.hugegraph.store.client.util.MetricX;

import lombok.extern.slf4j.Slf4j;

/**
 * 2022/2/14
 */
@Slf4j
public class HgStoreScanner {

    public static final byte[] EMPTY_BYTES = new byte[0];
    private final HgStoreClient storeClient;
    private final String graphName;
    private long modNumber = 1_000_000;
    private int max = 10_000_000;

    private HgStoreScanner(HgStoreClient storeClient, String graph) {
        this.storeClient = storeClient;
        this.graphName = graph;
    }

    public static HgStoreScanner of(HgStoreClient storeClient, String graph) {
        return new HgStoreScanner(storeClient, graph);
    }

    public long getModNumber() {
        return modNumber;
    }

    public void setModNumber(int modNumber) {
        if (modNumber <= 0) {
            return;
        }
        this.modNumber = modNumber;
    }

    public int getMax() {
        return max;
    }

    public void setMax(int max) {
        if (modNumber <= 0) {
            return;
        }
        this.max = max;
    }

    protected HgStoreSession getStoreSession() {
        return HgSessionManager.getInstance().openSession(this.graphName);
    }

    protected HgStoreSession getStoreSession(String graphName) {
        return HgSessionManager.getInstance().openSession(graphName);
    }

    public void scanTable(String tableName) {
        log.info("Starting scan table [{}] of graph [{}] ...", tableName, graphName);
        HgMetricX hgMetricX = HgMetricX.ofStart();
        HgStoreSession session = getStoreSession();
        int count = 0;
        KvCloseableIterator<HgKvIterator<HgKvEntry>> iterator =
                session.scanBatch2(HgScanQuery.tableOf(tableName));

        long start = System.currentTimeMillis();
        while (iterator.hasNext()) {
            HgKvIterator<HgKvEntry> iterator2 = iterator.next();
            while (iterator2.hasNext()) {

                count++;
                iterator2.next();
                if (count % (modNumber) == 0) {
                    log.info("Scanning keys: " + count + " time is " + modNumber * 1000
                                                                       /
                                                                       (System.currentTimeMillis() -
                                                                        start));
                    start = System.currentTimeMillis();
                }
                if (count == max) {
                    break;
                }

            }
        }
        iterator.close();

        hgMetricX.end();
        log.info("*************************************************");
        log.info("*************  Scanning Completed  **************");
        log.info("Graph: {}", graphName);
        log.info("Table: {}", tableName);
        log.info("Keys: {}", count);
        log.info("Max: {}", max);
        log.info("Waiting: {} seconds.", MetricX.getIteratorWait() / 1000);
        log.info("Total: {} seconds.", hgMetricX.past() / 1000);
        log.info("Iterator: [{}]", iterator.getClass().getSimpleName());
        log.info("Page: {}", HgStoreClientConfig.of().getNetKvScannerPageSize());
        log.info("*************************************************");
    }

    public void scanHash() {

        String tableName = "g+i";
        HgMetricX hgMetricX = HgMetricX.ofStart();
        String graphName = "/DEFAULT/graphs/hugegraph1/";
        HgStoreSession session = getStoreSession(graphName);
        int count = 0;
        String query =
                "{\"conditions\":[{\"cls\":\"S\",\"el\":{\"key\":\"ID\",\"relation\":\"SCAN\"," +
                "\"value\"" +
                ":{\"start\":\"61180\",\"end\":\"63365\",\"length\":0}}}]," +
                "\"optimizedType\":\"NONE\",\"ids\":[]," +
                "\"mustSortByInput\":true,\"resultType\":\"EDGE\",\"offset\":0," +
                "\"actualOffset\":0,\"actualStoreOffset\":" +
                "0,\"limit\":9223372036854775807,\"capacity\":-1,\"showHidden\":false," +
                "\"showDeleting\":false," +
                "\"showExpired\":false,\"olap\":false,\"withProperties\":false,\"olapPks\":[]}";
        //HgKvIterator<HgKvEntry> iterator = session.scanIterator(tableName,0,715827883,
        // HgKvStore.SCAN_ANY,null);

        //HgKvIterator<HgKvEntry> iterator = session.scanIterator(tableName,61180,63365, 348, null);
        //HgKvIterator<HgKvEntry> iterator = session.scanIterator(tableName,0,65535, 348, null);
        HgKvIterator<HgKvEntry> iterator = session.scanIterator(tableName);
        while (iterator.hasNext()) {

            count++;
            //iterator.next();
            // if (count % (modNumber) == 0) {
            //    log.info("Scanning keys: " + count);
            HgCliUtil.println(Arrays.toString(iterator.next().key()));
            //  }
            if (count == max) {
                break;
            }

        }

        hgMetricX.end();
        log.info("*************************************************");
        log.info("*************  Scanning Completed  **************");
        log.info("Graph: {}", this.graphName);
        log.info("Table: {}", tableName);
        log.info("Keys: {}", count);
        log.info("Max: {}", max);
        log.info("Waiting: {} seconds.", MetricX.getIteratorWait() / 1000);
        log.info("Total: {} seconds.", hgMetricX.past() / 1000);
        log.info("Iterator: [{}]", iterator.getClass().getSimpleName());
        log.info("Page: {}", HgStoreClientConfig.of().getNetKvScannerPageSize());
        log.info("*************************************************");
    }

    public void scanTable2(String tableName) throws PDException {
        // java -jar hg-store-cli-3.6.0-SNAPSHOT.jar -scan 10.45.30.212:8989 "DEFAULT/case_112/g"
        // g+ie
        PDClient pdClient = storeClient.getPdClient();
        List<Metapb.Partition> partitions = pdClient.getPartitions(0, graphName);
        HgStoreSession session = storeClient.openSession(graphName);
        int count = 0;
        byte[] position = null;
        HgMetricX hgMetricX = HgMetricX.ofStart();
        for (Metapb.Partition partition : partitions) {
            while (true) {
                try (HgKvIterator<HgKvEntry> iterator = session.scanIterator(tableName,
                                                                             (int) (partition.getStartKey()),
                                                                             (int) (partition.getEndKey()),
                                                                             HgKvStore.SCAN_HASHCODE,
                                                                             EMPTY_BYTES)) {
                    if (position != null) {
                        iterator.seek(position);
                    }
                    while (iterator.hasNext()) {
                        iterator.next();
                        count++;
                        if (count % 3000 == 0) {
                            if (iterator.hasNext()) {
                                iterator.next();
                                position = iterator.position();
                                System.out.println("count is " + count);
                            } else {
                                position = null;
                            }
                            break;
                        }
                    }
                    if (!iterator.hasNext()) {
                        position = null;
                        break;
                    }
                }
            }
        }
        hgMetricX.end();
        log.info("*************************************************");
        log.info("*************  Scanning Completed  **************");
        log.info("Graph: {}", graphName);
        log.info("Table: {}", tableName);
        log.info("Keys: {}", count);
        log.info("Total: {} seconds.", hgMetricX.past() / 1000);
        log.info("*************************************************");
    }

}
