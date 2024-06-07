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

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hugegraph.store.meta.base.DBSessionBuilder;
import org.apache.hugegraph.store.meta.base.PartitionMetaStore;
import org.apache.hugegraph.store.util.HgStoreException;

import com.google.protobuf.Int64Value;

/**
 * GraphId管理器，维护一个自增循环ID，负责管理GraphName和GraphId的映射
 */
public class GraphIdManager extends PartitionMetaStore {

    protected static final String GRAPH_ID_PREFIX = "@GRAPH_ID@";
    protected static int maxGraphID = 65535;
    static Object graphIdLock = new Object();
    static Object cidLock = new Object();
    final DBSessionBuilder sessionBuilder;
    final int partitionId;
    // public long getGraphId(String graphName) {
    //    if (!graphIdCache.containsKey(graphName)) {
    //        synchronized (graphIdLock) {
    //            if (!graphIdCache.containsKey(graphName)) {
    //                byte[] key = MetadataKeyHelper.getGraphIDKey(graphName);
    //                Int64Value id = get(Int64Value.parser(), key);
    //                if (id == null) {
    //                    id = Int64Value.of(getCId(GRAPH_ID_PREFIX, maxGraphID));
    //                    if (id.getValue() == -1) {
    //                        throw new HgStoreException(HgStoreException.EC_FAIL,
    //                                "The number of graphs exceeds the maximum 65535");
    //                    }
    //                    put(key, id);
    //                    flush();
    //                }
    //                graphIdCache.put(graphName, id.getValue());
    //            }
    //        }
    //    }
    //    return graphIdCache.get(graphName);
    // }
    private final Map<String, Long> graphIdCache = new ConcurrentHashMap<>();

    public GraphIdManager(DBSessionBuilder sessionBuilder, int partitionId) {
        super(sessionBuilder, partitionId);
        this.sessionBuilder = sessionBuilder;
        this.partitionId = partitionId;
    }

    /**
     * 获取一个图的id
     */
    public long getGraphId(String graphName) {
        Long l = graphIdCache.get(graphName);
        if (l == null) {
            synchronized (graphIdLock) {
                if ((l = graphIdCache.get(graphName)) == null) {
                    byte[] key = MetadataKeyHelper.getGraphIDKey(graphName);
                    Int64Value id = get(Int64Value.parser(), key);
                    if (id == null) {
                        id = Int64Value.of(getCId(GRAPH_ID_PREFIX, maxGraphID));
                        if (id.getValue() == -1) {
                            throw new HgStoreException(HgStoreException.EC_FAIL,
                                                       "The number of graphs exceeds the maximum " +
                                                       "65535");
                        }
                        put(key, id);
                        flush();
                    }
                    l = id.getValue();
                    graphIdCache.put(graphName, l);
                }
            }
        }
        return l;
    }

    /**
     * 释放一个图id
     */
    public long releaseGraphId(String graphName) {
        long gid = getGraphId(graphName);
        synchronized (graphIdLock) {
            graphIdCache.remove(graphName);
            byte[] key = MetadataKeyHelper.getGraphIDKey(graphName);
            delete(key);
            delCId(GRAPH_ID_PREFIX, gid);
            flush();
        }
        return gid;
    }

    /**
     * 获取自增循环不重复id, 达到上限后从0开始自增
     *
     * @param key key
     * @param max id上限，达到该值后，重新从0开始自增
     * @return id
     */
    protected long getCId(String key, long max) {
        byte[] cidNextKey = MetadataKeyHelper.getCidKey(key);
        synchronized (cidLock) {
            Int64Value value = get(Int64Value.parser(), cidNextKey);
            long current = value != null ? value.getValue() : 0L;
            long last = current == 0 ? max - 1 : current - 1;
            // 查找一个未使用的cid
            List<Int64Value> ids =
                    scan(Int64Value.parser(), genCIDSlotKey(key, current), genCIDSlotKey(key, max));
            for (Int64Value id : ids) {
                if (current == id.getValue()) {
                    current++;
                } else {
                    break;
                }
            }

            if (current == max) {
                current = 0;
                ids = scan(Int64Value.parser(), genCIDSlotKey(key, current),
                           genCIDSlotKey(key, last));
                for (Int64Value id : ids) {
                    if (current == id.getValue()) {
                        current++;
                    } else {
                        break;
                    }
                }
            }

            if (current == last) {
                return -1;
            }
            // 保存当前id，标记已被使用
            put(genCIDSlotKey(key, current), Int64Value.of(current));
            // 保存下一次遍历的id
            put(cidNextKey, Int64Value.of(current + 1));
            return current;
        }
    }

    /**
     * 返回已使用Cid的key
     */
    private byte[] genCIDSlotKey(String key, long value) {
        byte[] keySlot = MetadataKeyHelper.getCidSlotKeyPrefix(key);
        ByteBuffer buf = ByteBuffer.allocate(keySlot.length + Long.SIZE);
        buf.put(keySlot);
        buf.putLong(value);
        return buf.array();
    }

    /**
     * 删除一个循环id，释放id值
     */
    protected void delCId(String key, long cid) {
        delete(genCIDSlotKey(key, cid));
    }

}
