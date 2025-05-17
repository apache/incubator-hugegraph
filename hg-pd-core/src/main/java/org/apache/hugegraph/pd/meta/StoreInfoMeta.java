<<<<<<<< HEAD:hugegraph-pd/hg-pd-core/src/main/java/org/apache/hugegraph/pd/meta/StoreInfoMeta.java
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

package org.apache.hugegraph.pd.meta;
========
package org.apache.hugegraph.pd.meta;

import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.config.PDConfig;
import org.apache.hugegraph.pd.grpc.Metapb;
import lombok.extern.slf4j.Slf4j;
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-core/src/main/java/org/apache/hugegraph/pd/meta/StoreInfoMeta.java

import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.hugegraph.pd.common.Consts.DEFAULT_STORE_GROUP_ID;

import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.config.PDConfig;
import org.apache.hugegraph.pd.grpc.Metapb;

import lombok.extern.slf4j.Slf4j;

/**
 * Store information storage
 */
@Slf4j
public class StoreInfoMeta extends MetadataRocksDBStore {

<<<<<<<< HEAD:hugegraph-pd/hg-pd-core/src/main/java/org/apache/hugegraph/pd/meta/StoreInfoMeta.java
========
    private PDConfig pdConfig;
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-core/src/main/java/org/apache/hugegraph/pd/meta/StoreInfoMeta.java

    public StoreInfoMeta(PDConfig pdConfig) {
        super(pdConfig);
        //   this.timeout = pdConfig.getDiscovery().getHeartbeatOutTimes();
    }

    public static boolean shardGroupEquals(List<Metapb.Shard> g1, List<Metapb.Shard> g2) {
        ListIterator<Metapb.Shard> e1 = g1.listIterator();
        ListIterator<Metapb.Shard> e2 = g2.listIterator();
        while (e1.hasNext() && e2.hasNext()) {
            Metapb.Shard o1 = e1.next();
            Metapb.Shard o2 = e2.next();
            if (!(o1 == null ? o2 == null : o1.getStoreId() == o2.getStoreId())) {
                return false;
            }
        }
        return !(e1.hasNext() || e2.hasNext());
    }

    /**
     * Update the Store information
     *
     * @param store
     * @throws PDException
     */
    public void updateStore(Metapb.Store store) throws PDException {
        byte[] storeInfoKey = MetadataKeyHelper.getStoreInfoKey(store.getId());
        put(storeInfoKey, store.toByteArray());
    }

    /**
     * Update the survivability status of the store
     *
     * @param store
     */
    public void keepStoreAlive(Metapb.Store store) throws PDException {
        byte[] activeStoreKey = MetadataKeyHelper.getActiveStoreKey(store.getId());
        putWithTTL(activeStoreKey, store.toByteArray(), pdConfig.getStore().getKeepAliveTimeout());
    }

    public void removeActiveStore(Metapb.Store store) throws PDException {
        byte[] activeStoreKey = MetadataKeyHelper.getActiveStoreKey(store.getId());
        removeWithTTL(activeStoreKey);
    }

    public Metapb.Store getStore(Long storeId) throws PDException {
        byte[] storeInfoKey = MetadataKeyHelper.getStoreInfoKey(storeId);
        Metapb.Store store = getOne(Metapb.Store.parser(), storeInfoKey);
        return store;
    }

    /**
     * Get all the stores
     *
     * @param graphName
     * @return
     * @throws PDException
     */
    @Deprecated
    public List<Metapb.Store> getStores(String graphName) throws PDException {
        byte[] storePrefix = MetadataKeyHelper.getStorePrefix();
        return scanPrefix(Metapb.Store.parser(), storePrefix);
<<<<<<<< HEAD:hugegraph-pd/hg-pd-core/src/main/java/org/apache/hugegraph/pd/meta/StoreInfoMeta.java
========
    }

    public List<Metapb.Store> getAllStores() throws PDException {
        byte[] storePrefix = MetadataKeyHelper.getStorePrefix();
        return scanPrefix(Metapb.Store.parser(), storePrefix);
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-core/src/main/java/org/apache/hugegraph/pd/meta/StoreInfoMeta.java
    }

    /**
     * Get an active store
     *
     * @return
     * @throws PDException
     */
<<<<<<<< HEAD:hugegraph-pd/hg-pd-core/src/main/java/org/apache/hugegraph/pd/meta/StoreInfoMeta.java
    public List<Metapb.Store> getActiveStores(String graphName) throws PDException {
        byte[] activePrefix = MetadataKeyHelper.getActiveStorePrefix();
        List listWithTTL = getInstanceListWithTTL(Metapb.Store.parser(),
                                                  activePrefix);
        return listWithTTL;
========
    public List<Metapb.Store> getActiveStores(int storeGroupId) throws PDException {
        Set<Long> storeIds = getStoreIdsByGroup(storeGroupId);
        return getActiveStores().stream()
                .filter(store -> storeIds.contains(store.getId()))
                .collect(Collectors.toList());
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-core/src/main/java/org/apache/hugegraph/pd/meta/StoreInfoMeta.java
    }

    public List<Metapb.Store> getActiveStores() throws PDException {
        byte[] activePrefix = MetadataKeyHelper.getActiveStorePrefix();
        List listWithTTL = getInstanceListWithTTL(Metapb.Store.parser(),
                                                  activePrefix);
        return listWithTTL;
    }

    /**
     * Check whether the storeID exists
     *
     * @param storeId
     * @return
     */
    public boolean storeExists(Long storeId) throws PDException {
        byte[] storeInfoKey = MetadataKeyHelper.getStoreInfoKey(storeId);
        return containsKey(storeInfoKey);
    }

    /**
     * Update the storage status information
     *
     * @param storeStats
     */
    public Metapb.StoreStats updateStoreStats(Metapb.StoreStats storeStats) throws PDException {
        byte[] storeStatusKey = MetadataKeyHelper.getStoreStatusKey(storeStats.getStoreId());

        put(storeStatusKey, storeStats.toByteArray());
        return storeStats;
    }

    public long removeStore(long storeId) throws PDException {
        byte[] storeInfoKey = MetadataKeyHelper.getStoreInfoKey(storeId);
        return remove(storeInfoKey);
    }

    public long removeAll() throws PDException {
        byte[] storePrefix = MetadataKeyHelper.getStorePrefix();
        return this.removeByPrefix(storePrefix);
    }

    public void updateShardGroup(Metapb.ShardGroup group) throws PDException {
        byte[] shardGroupKey = MetadataKeyHelper.getShardGroupKey(group.getId());
        put(shardGroupKey, group.toByteArray());
    }

    public void deleteShardGroup(int groupId) throws PDException {
        byte[] shardGroupKey = MetadataKeyHelper.getShardGroupKey(groupId);
        remove(shardGroupKey);
    }

    public Metapb.ShardGroup getShardGroup(int groupId) throws PDException {
        byte[] shardGroupKey = MetadataKeyHelper.getShardGroupKey(groupId);
        return getOne(Metapb.ShardGroup.parser(), shardGroupKey);
    }

    public int getShardGroupCount() throws PDException {
        byte[] shardGroupPrefix = MetadataKeyHelper.getShardGroupPrefix();
        return scanPrefix(Metapb.ShardGroup.parser(), shardGroupPrefix).size();
    }

    public List<Metapb.ShardGroup> getShardGroups() throws PDException {
        byte[] shardGroupPrefix = MetadataKeyHelper.getShardGroupPrefix();
        return scanPrefix(Metapb.ShardGroup.parser(), shardGroupPrefix);
    }

    public Metapb.StoreStats getStoreStats(long storeId) throws PDException {
        byte[] storeStatusKey = MetadataKeyHelper.getStoreStatusKey(storeId);
        Metapb.StoreStats stats = getOne(Metapb.StoreStats.parser(),
                                         storeStatusKey);
        return stats;
    }

    /**
     * @return store and status information
     * @throws PDException
     */
    public List<Metapb.Store> getStoreStatus(boolean isActive) throws PDException {
        byte[] storePrefix = MetadataKeyHelper.getStorePrefix();
<<<<<<<< HEAD:hugegraph-pd/hg-pd-core/src/main/java/org/apache/hugegraph/pd/meta/StoreInfoMeta.java
        List<Metapb.Store> stores = isActive ? getActiveStores() :
                                    scanPrefix(Metapb.Store.parser(), storePrefix);
========
        List<Metapb.Store> stores =isActive ? getActiveStores() :
                                   scanPrefix(Metapb.Store.parser(), storePrefix);
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-core/src/main/java/org/apache/hugegraph/pd/meta/StoreInfoMeta.java
        LinkedList<Metapb.Store> list = new LinkedList<>();
        for (int i = 0; i < stores.size(); i++) {
            Metapb.Store store = stores.get(i);
            Metapb.StoreStats stats = getStoreStats(store.getId());
            if (stats != null) {
<<<<<<<< HEAD:hugegraph-pd/hg-pd-core/src/main/java/org/apache/hugegraph/pd/meta/StoreInfoMeta.java
                store = Metapb.Store.newBuilder(store).setStats(getStoreStats(store.getId()))
                                    .build();
========
                store = Metapb.Store.newBuilder(store).setStats(getStoreStats(store.getId())) .build();
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-core/src/main/java/org/apache/hugegraph/pd/meta/StoreInfoMeta.java
            }
            list.add(store);
        }
        return list;
    }

    public void updateStoreGroup(Long storeId, int storeGroupId) throws PDException {
        byte[] groupKey = MetadataKeyHelper.getStoreGroupRelationKey(storeId);
        put(groupKey, String.valueOf(storeGroupId).getBytes());
    }

    public Set<Long> getStoreIdsByGroup(int storeGroupId) throws PDException {
        byte[] groupKey = MetadataKeyHelper.getStoreGroupRelationPrefix();
        return scanPrefix(groupKey).stream()
                .filter(g -> Objects.equals(storeGroupId, Integer.parseInt(new String(g.getValue()))))
                .map(g -> Long.parseLong(new String(g.getKey()).split("/")[1]))
                .collect(Collectors.toSet());
    }

    public int getStoreGroupByStoreId(long storeId) throws PDException {
        byte[] storeInfoKey = MetadataKeyHelper.getStoreGroupRelationKey(storeId);
        byte[] value = getOne(storeInfoKey);
        return value == null ? DEFAULT_STORE_GROUP_ID : Integer.parseInt(new String(value));
    }

    public boolean isStoreHasGroup(long storeId) throws PDException {
        byte[] groupKey = MetadataKeyHelper.getStoreGroupRelationKey(storeId);
        return getOne(groupKey) != null;
    }
}
