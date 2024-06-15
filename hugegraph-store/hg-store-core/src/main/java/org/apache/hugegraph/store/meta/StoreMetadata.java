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

package org.apache.hugegraph.store.meta;

import java.io.File;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.store.meta.base.GlobalMetaStore;
import org.apache.hugegraph.store.options.MetadataOptions;
import org.apache.hugegraph.store.util.HgStoreException;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StoreMetadata extends GlobalMetaStore {

    protected final static int Store_DataFmt_Version = 1;
    private final List<String> dataLocations;
    private final List<String> raftLocations;
    private Store store = null;

    public StoreMetadata(MetadataOptions options) {
        super(options);
        dataLocations = Arrays.asList(options.getDataPath().split(","));
        raftLocations = Arrays.asList(options.getRaftPath().split(","));
    }

    public List<String> getDataLocations() {
        return dataLocations;
    }

    public List<String> getRaftLocations() {
        return raftLocations;
    }

    public Store load() {
        // 针对多目录存储的情况下，预先创建文件夹，方便pd端统计文件存储
        dataLocations.forEach(path -> {
            String strPath = Paths.get(path).toAbsolutePath().toString();
            File dbFile = new File(strPath);
            if (!dbFile.exists()) {
                dbFile.mkdir();
            }
        });

        raftLocations.forEach(path -> {
            String strPath = Paths.get(path).toAbsolutePath().toString();
            File dbFile = new File(strPath);
            if (!dbFile.exists()) {
                dbFile.mkdir();
            }
        });

        byte[] key = MetadataKeyHelper.getStoreKey();
        byte[] value = get(key);
        if (value != null) {
            try {
                Metapb.Store protoObj = Metapb.Store.parseFrom(value);
                if (protoObj != null) {
                    store = new Store(protoObj);
                }
            } catch (Exception e) {
                throw new HgStoreException(HgStoreException.EC_FAIL, e);
            }
        }
        if (store == null) {
            store = new Store(Store_DataFmt_Version);
        }
        checkDataFmtCompatible();
        return store;
    }

    public Store getStore() {
        return store;
    }

    public void save(Store store) {
        byte[] key = MetadataKeyHelper.getStoreKey();
        put(key, store.getProtoObj().toByteArray());
        this.store = store;
    }

    public void checkDataFmtCompatible() {
        if (store == null || store.getDataVersion() != Store_DataFmt_Version) {
            throw new HgStoreException(HgStoreException.EC_DATAFMT_NOT_SUPPORTED,
                                       String.format(
                                               "Incompatible data format, data format version is " +
                                               "%d, supported version is %d",
                                               store.getDataVersion(), Store_DataFmt_Version));
        }
    }

    public Metapb.PartitionStore getPartitionStore(int partitionId) {
        byte[] key = MetadataKeyHelper.getPartitionStoreKey(partitionId);
        return get(Metapb.PartitionStore.parser(), key);
    }

    public List<Metapb.PartitionStore> getPartitionStores() {
        byte[] key = MetadataKeyHelper.getPartitionStorePrefix();
        return scan(Metapb.PartitionStore.parser(), key);
    }

    public void savePartitionStore(Metapb.PartitionStore partitionStore) {
        byte[] key = MetadataKeyHelper.getPartitionStoreKey(partitionStore.getPartitionId());
        put(key, partitionStore.toByteArray());
    }

    public Metapb.PartitionRaft getPartitionRaft(int partitionId) {
        byte[] key = MetadataKeyHelper.getPartitionRaftKey(partitionId);
        return get(Metapb.PartitionRaft.parser(), key);
    }

    public List<Metapb.PartitionRaft> getPartitionRafts() {
        byte[] key = MetadataKeyHelper.getPartitionRaftPrefix();
        return scan(Metapb.PartitionRaft.parser(), key);
    }

    public void savePartitionRaft(Metapb.PartitionRaft partitionRaft) {
        byte[] key = MetadataKeyHelper.getPartitionRaftKey(partitionRaft.getPartitionId());
        put(key, partitionRaft.toByteArray());
    }

    private String getMinDataLocation() {
        Map<String, Integer> counter = new HashMap<>();
        dataLocations.forEach(l -> {
            counter.put(l, Integer.valueOf(0));
        });
        getPartitionStores().forEach(ptStore -> {
            if (counter.containsKey(ptStore.getStoreLocation())) {
                counter.put(ptStore.getStoreLocation(),
                            counter.get(ptStore.getStoreLocation()) + 1);
            }
        });
        int min = Integer.MAX_VALUE;
        String location = "";
        for (String k : counter.keySet()) {
            if (counter.get(k) < min) {
                min = counter.get(k);
                location = k;
            }
        }
        return location;
    }

    private String getMinRaftLocation() {
        Map<String, Integer> counter = new HashMap<>();
        raftLocations.forEach(l -> {
            counter.put(l, Integer.valueOf(0));
        });

        getPartitionRafts().forEach(ptRaft -> {
            if (counter.containsKey(ptRaft.getRaftLocation())) {
                counter.put(ptRaft.getRaftLocation(), counter.get(ptRaft.getRaftLocation()) + 1);
            }
        });

        int min = Integer.MAX_VALUE;
        String location = "";
        for (String k : counter.keySet()) {
            if (counter.get(k) < min) {
                min = counter.get(k);
                location = k;
            }
        }
        return location;
    }

    /**
     * 获取分区数据存储的位置，如果分布数据不存在，自动创建新的位置
     *
     * @param partitionId
     * @return
     */
    public String getPartitionStoreLocation(int partitionId, String dbName) {
        Metapb.PartitionStore location = getPartitionStore(partitionId);
        if (location == null) {
            synchronized (this) {
                location = getPartitionStore(partitionId);
                if (location == null) {
                    // 查找分区数最少的存储
                    location = Metapb.PartitionStore.newBuilder()
                                                    .setPartitionId(partitionId)
                                                    .setStoreLocation(getMinDataLocation())
                                                    .build();
                    // TODO 选择分区数最小的路径
                    savePartitionStore(location);
                }
            }
        }
        return location.getStoreLocation();
    }

    public String getPartitionRaftLocation(int partitionId) {
        Metapb.PartitionRaft location = getPartitionRaft(partitionId);
        if (location == null) {
            synchronized (this) {
                location = getPartitionRaft(partitionId);
                if (location == null) {
                    // 查找分区数最少的存储
                    location = Metapb.PartitionRaft.newBuilder()
                                                   .setPartitionId(partitionId)
                                                   .setRaftLocation(getMinRaftLocation())
                                                   .build();
                    // TODO 选择分区数最小的路径
                    savePartitionRaft(location);
                }
            }
        }
        return location.getRaftLocation();
    }
}
