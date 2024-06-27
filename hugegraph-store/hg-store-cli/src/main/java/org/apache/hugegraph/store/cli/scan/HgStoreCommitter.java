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

import org.apache.hugegraph.store.HgOwnerKey;
import org.apache.hugegraph.store.HgSessionManager;
import org.apache.hugegraph.store.HgStoreSession;
import org.apache.hugegraph.store.cli.util.HgCliUtil;
import org.apache.hugegraph.store.client.HgStoreNodeManager;

/**
 * 2022/2/28
 */
public class HgStoreCommitter {

    protected final static HgStoreNodeManager nodeManager = HgStoreNodeManager.getInstance();

    private final String graph;

    private HgStoreCommitter(String graph) {
        this.graph = graph;
    }

    public static HgStoreCommitter of(String graph) {
        return new HgStoreCommitter(graph);
    }

    protected HgStoreSession getStoreSession() {
        return HgSessionManager.getInstance().openSession(this.graph);
    }

    protected HgStoreSession getStoreSession(String graphName) {
        return HgSessionManager.getInstance().openSession(graphName);
    }

    public void put(String tableName, int amount) {
        //*************** Put Benchmark **************//*
        String keyPrefix = "PUT-BENCHMARK";
        HgStoreSession session = getStoreSession();

        int length = String.valueOf(amount).length();

        session.beginTx();

        long start = System.currentTimeMillis();
        for (int i = 0; i < amount; i++) {
            HgOwnerKey key = HgCliUtil.toOwnerKey(
                    keyPrefix + "-" + HgCliUtil.padLeftZeros(String.valueOf(i), length));
            byte[] value = HgCliUtil.toBytes(keyPrefix + "-V-" + i);

            session.put(tableName, key, value);

            if ((i + 1) % 100_000 == 0) {
                HgCliUtil.println("---------- " + (i + 1) + " --------");
                HgCliUtil.println(
                        "Preparing took: " + (System.currentTimeMillis() - start) + " ms.");
                session.commit();
                HgCliUtil.println(
                        "Committing took: " + (System.currentTimeMillis() - start) + " ms.");
                start = System.currentTimeMillis();
                session.beginTx();
            }
        }

        if (session.isTx()) {
            session.commit();
        }

    }
}
