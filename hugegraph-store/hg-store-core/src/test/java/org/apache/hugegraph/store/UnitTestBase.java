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

package org.apache.hugegraph.store;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.apache.hugegraph.rocksdb.access.RocksDBFactory;
import org.apache.hugegraph.rocksdb.access.RocksDBSession;
import org.apache.hugegraph.store.business.BusinessHandler;
import org.apache.hugegraph.store.business.BusinessHandlerImpl;
import org.apache.hugegraph.store.meta.PartitionManager;
import org.apache.hugegraph.store.options.HgStoreEngineOptions;
import org.apache.hugegraph.store.options.RaftRocksdbOptions;
import org.apache.hugegraph.store.pd.FakePdServiceProvider;
import org.apache.hugegraph.store.pd.PdProvider;

public class UnitTestBase {

    public final static RocksDBFactory factory = RocksDBFactory.getInstance();
    private String dbPath;

    private BusinessHandler handler;

    public static boolean deleteDir(File dir) {
        if (dir.isDirectory()) {
            for (File file : dir.listFiles()) {
                deleteDir(file);
            }
        }
        return dir.delete();
    }

    public void initDB(String dbPath) {
        this.dbPath = dbPath;
        UnitTestBase.deleteDir(new File(dbPath));
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("rocksdb.write_buffer_size", "1048576");
        configMap.put("rocksdb.bloom_filter_bits_per_key", "10");

        RaftRocksdbOptions.initRocksdbGlobalConfig(configMap);
        BusinessHandlerImpl.initRocksdb(configMap, null);


    }

    protected BusinessHandler getBusinessHandler() {
        if (handler == null) {
            synchronized (this) {
                if (handler == null) {
                    int partitionCount = 2;
                    HgStoreEngineOptions options = new HgStoreEngineOptions() {{
                        setDataPath(dbPath);
                        setFakePdOptions(new HgStoreEngineOptions.FakePdOptions() {{
                            setPartitionCount(partitionCount);
                            setPeersList("127.0.0.1");
                            setStoreList("127.0.0.1");

                        }});
                    }};

                    PdProvider pdProvider = new FakePdServiceProvider(options.getFakePdOptions());
                    PartitionManager partitionManager = new PartitionManager(pdProvider, options);
                    BusinessHandler handler = new BusinessHandlerImpl(partitionManager);
                }
            }
        }

        return handler;
    }

    public RocksDBSession getDBSession(String dbName) {
        RocksDBSession session = factory.queryGraphDB(dbName);
        if (session == null) {
            session = factory.createGraphDB(dbPath, dbName);
        }
        return session;
    }

    public void close() {
        handler.closeAll();
    }
}
