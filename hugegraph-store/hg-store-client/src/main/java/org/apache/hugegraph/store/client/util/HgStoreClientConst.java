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

package org.apache.hugegraph.store.client.util;

import java.util.Collections;
import java.util.List;

import org.apache.hugegraph.store.HgKvStore;
import org.apache.hugegraph.store.HgOwnerKey;


public final class HgStoreClientConst {
    public final static String DEFAULT_NODE_CLUSTER_ID = "default-node-cluster";

    public final static String EMPTY_STRING = "";
    public final static String EMPTY_TABLE = "";
    public final static byte[] EMPTY_BYTES = new byte[0];
    public final static byte[] MAX_BYTES = new byte[]{(byte) 0b11111111};
    public final static List EMPTY_LIST = Collections.EMPTY_LIST;

    public final static byte[] ALL_PARTITION_OWNER = new byte[0];
    // means to dispatch to all partitions.
    public final static HgOwnerKey EMPTY_OWNER_KEY = HgOwnerKey.of(EMPTY_BYTES, EMPTY_BYTES);
    public final static HgOwnerKey ALL_PARTITION_OWNER_KEY =
            HgOwnerKey.of(ALL_PARTITION_OWNER, ALL_PARTITION_OWNER);

    //public final static int SCAN_GTE_BEGIN_LT_END = SCAN_GTE_BEGIN | SCAN_LT_END;
    public final static int SCAN_TYPE_RANGE = HgKvStore.SCAN_GTE_BEGIN | HgKvStore.SCAN_LTE_END;
    public final static int SCAN_TYPE_ANY = HgKvStore.SCAN_ANY;
    public final static int NO_LIMIT = 0;

    public final static int TX_SESSIONS_MAP_CAPACITY = 32;
    public static final int NODE_MAX_RETRYING_TIMES = 10;


}
