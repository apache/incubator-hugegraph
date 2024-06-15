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

import org.apache.hugegraph.pd.config.PDConfig;
import org.apache.hugegraph.pd.raft.RaftEngine;
import org.apache.hugegraph.pd.store.HgKVStore;
import org.apache.hugegraph.pd.store.HgKVStoreImpl;
import org.apache.hugegraph.pd.store.RaftKVStore;

/**
 * Storage Factory class to create a storage class for related objects
 */
public class MetadataFactory {

    private static HgKVStore store = null;

    public static HgKVStore getStore(PDConfig pdConfig) {
        if (store == null) {
            synchronized (MetadataFactory.class) {
                if (store == null) {
                    HgKVStore proto = new HgKVStoreImpl();
                    //proto.init(pdConfig);
                    store = pdConfig.getRaft().isEnable() ?
                            new RaftKVStore(RaftEngine.getInstance(), proto) :
                            proto;
                    store.init(pdConfig);
                }
            }
        }
        return store;
    }

    public static void closeStore() {
        if (store != null) {
            store.close();
        }
    }

    public static StoreInfoMeta newStoreInfoMeta(PDConfig pdConfig) {
        return new StoreInfoMeta(pdConfig);
    }

    public static PartitionMeta newPartitionMeta(PDConfig pdConfig) {
        return new PartitionMeta(pdConfig);
    }

    public static IdMetaStore newHugeServerMeta(PDConfig pdConfig) {
        return new IdMetaStore(pdConfig);
    }

    public static DiscoveryMetaStore newDiscoveryMeta(PDConfig pdConfig) {
        return new DiscoveryMetaStore(pdConfig);
    }

    public static ConfigMetaStore newConfigMeta(PDConfig pdConfig) {
        return new ConfigMetaStore(pdConfig);
    }

    public static TaskInfoMeta newTaskInfoMeta(PDConfig pdConfig) {
        return new TaskInfoMeta(pdConfig);
    }

    public static QueueStore newQueueStore(PDConfig pdConfig) {
        return new QueueStore(pdConfig);
    }

    public static LogMeta newLogMeta(PDConfig pdConfig) {
        return new LogMeta(pdConfig);
    }
}
