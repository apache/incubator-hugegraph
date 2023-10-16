/*
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

package org.apache.hugegraph.example;

import static org.apache.hugegraph.backend.page.PageState.EMPTY_BYTES;

import org.apache.hugegraph.pd.client.PDConfig;
import org.apache.hugegraph.store.HgKvEntry;
import org.apache.hugegraph.store.HgKvIterator;
import org.apache.hugegraph.store.HgKvStore;
import org.apache.hugegraph.store.HgStoreClient;
import org.apache.hugegraph.store.HgStoreSession;

public class ExampleNew {
    public static void main(String[] args) throws Exception {
        testScanTable("hugegraph", "g+v"); // why should with "g+"?
    }

    private static void testScanTable(String graph, String table) {
        /*
         * Valid table is:
         * g+v
         * g+oe
         * g+ie
         * g+olap
         * g+task
         * g+index
         * g+server
         */
        HgStoreClient storeClient = HgStoreClient.create(
            PDConfig.of("127.0.0.1:8686").setEnableCache(false));
        String storeTemplate = "%s/g";
        String store = String.format(storeTemplate, graph);
        HgStoreSession session = storeClient.openSession(store);

        try (HgKvIterator<HgKvEntry> iterators = session.scanIterator(table,
                                                                      0, 100000000,
                                                                      HgKvStore.SCAN_HASHCODE,
                                                                      EMPTY_BYTES)) {
            int count = 0;
            while (iterators.hasNext()) {
                count++;
                HgKvEntry next = iterators.next();
                System.out.println(new String(next.key()) +
                                   " <====> " +
                                   new String(next.value()));
            }
            System.out.println(count);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
