/*
 * Copyright 2017 HugeGraph Authors
 *
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

package com.baidu.hugegraph.backend.store.rocksdb;

import java.util.ArrayList;
import java.util.List;

import com.baidu.hugegraph.backend.serializer.BytesBuffer;
import com.baidu.hugegraph.backend.store.rocksdb.RocksDBSessions.Session;
import com.baidu.hugegraph.type.Shard;
import com.baidu.hugegraph.util.Bytes;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.NumericUtil;

public class RocksDBRange {

    // The minimal shard size should >= 1M to prevent too many number of shards
    private static final int MIN_SHARD_SIZE = (int) Bytes.MB;

    // We assume the size of each key-value is 100 bytes
    private static final int ESTIMATE_BYTES_PER_KV = 100;

    private static final String MEM_SIZE = "rocksdb.size-all-mem-tables";
    private static final String SST_SIZE = "rocksdb.total-sst-files-size";

    private static final String NUM_KEYS = "rocksdb.estimate-num-keys";

    private final Session session;
    private final String table;

    public RocksDBRange(Session session, String table) {
        this.session = session;
        this.table = table;
    }

    public List<Shard> getSplits(long splitSize) {
        E.checkArgument(splitSize >= MIN_SHARD_SIZE,
                        "The split-size must be >= %s bytes, but got %s",
                        MIN_SHARD_SIZE, splitSize);

        long size = this.estimateDataSize();
        if (size <= 0) {
            size = this.estimateNumKeys() * ESTIMATE_BYTES_PER_KV;
        }

        double count = Math.ceil(size / (double) splitSize);
        if (count <= 0) {
            count = 1;
        }
        double each = BytesBuffer.UINT32_MAX / count;

        long offset = 0L;
        String last = position(offset);
        List<Shard> splits = new ArrayList<>((int) count);
        while (offset < BytesBuffer.UINT32_MAX) {
            offset += each;
            if (offset > BytesBuffer.UINT32_MAX) {
                offset = BytesBuffer.UINT32_MAX;
            }
            String current = position(offset);
            splits.add(new Shard(last, current, 0L));
            last = current;
        }
        return splits;
    }

    public long estimateDataSize() {
        long mem = Long.valueOf(this.session.property(this.table, MEM_SIZE));
        long sst = Long.valueOf(this.session.property(this.table, SST_SIZE));
        return mem + sst;
    }

    public long estimateNumKeys() {
        return Long.valueOf(this.session.property(this.table, NUM_KEYS));
    }

    public static String position(long position) {
        return String.valueOf(position);
    }

    public static byte[] position(String position) {
        int value = Long.valueOf(position).intValue();
        return NumericUtil.intToBytes(value);
    }
}
