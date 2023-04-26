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

package org.apache.hugegraph.store.client.grpc;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hugegraph.store.HgKvEntry;
import org.apache.hugegraph.store.HgOwnerKey;
import org.apache.hugegraph.store.client.HgStoreNodeSession;
import org.apache.hugegraph.store.client.util.HgStoreClientConst;
import org.apache.hugegraph.store.grpc.common.Header;
import org.apache.hugegraph.store.grpc.common.Key;
import org.apache.hugegraph.store.grpc.common.Kv;
import org.apache.hugegraph.store.grpc.common.Tk;
import org.apache.hugegraph.store.grpc.common.Tkv;
import org.apache.hugegraph.store.grpc.common.Tp;
import org.apache.hugegraph.store.grpc.common.Tse;

import com.google.protobuf.ByteString;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;

/**
 * 2022/1/19
 */
final class GrpcUtil {

    private static final ThreadLocal<Key.Builder> keyBuilder = new ThreadLocal<Key.Builder>();

    static Header getHeader(HgStoreNodeSession nodeSession) {
        return Header.newBuilder()
                     .setGraph(nodeSession.getGraphName())
                     .build();
    }

    static Tk toTk(String table, HgOwnerKey ownerKey) {
        return Tk.newBuilder()
                 .setTable(table)
                 .setKey(ByteString.copyFrom(ownerKey.getKey()))
                 .setCode(ownerKey.getKeyCode())
                 .build();
    }

    static Key.Builder getOwnerKeyBuilder() {
        Key.Builder builder = keyBuilder.get();
        if (builder == null) {
            builder = Key.newBuilder();
            // TODO 线程级变量，寻找删除时机
            keyBuilder.set(builder);
        }
        return builder;
    }

    static Key toKey(HgOwnerKey ownerKey, Key.Builder builder) {
        if (ownerKey == null) {
            return null;
        }
        return builder
                .setKey(ByteString.copyFrom(ownerKey.getKey()))
                .setCode(ownerKey.getKeyCode())
                .build();
    }


    static Key toKey(HgOwnerKey ownerKey) {
        if (ownerKey == null) {
            return null;
        }
        Key.Builder builder = keyBuilder.get();
        if (builder == null) {
            builder = Key.newBuilder();
            // TODO 线程级变量，寻找删除时机
            keyBuilder.set(builder);
        }
        return builder
                .setKey(ByteString.copyFrom(ownerKey.getKey()))
                .setCode(ownerKey.getKeyCode())
                .build();
    }

    static Tkv toTkv(String table, HgOwnerKey ownerKey, byte[] value) {
        return Tkv.newBuilder()
                  .setTable(table)
                  .setKey(ByteString.copyFrom(ownerKey.getKey()))
                  .setValue(ByteString.copyFrom(value))
                  .setCode(ownerKey.getKeyCode())
                  .build();
    }

    static Tp toTp(String table, HgOwnerKey ownerKey) {
        return Tp.newBuilder()
                 .setTable(table)
                 .setPrefix(ByteString.copyFrom(ownerKey.getKey()))
                 .setCode(ownerKey.getKeyCode())
                 .build();
    }

    static Tse toTse(String table, HgOwnerKey startKey, HgOwnerKey endKey) {
        return Tse.newBuilder()
                  .setTable(table)
                  .setStart(toKey(startKey))
                  .setEnd(toKey(endKey))
                  .build();

    }

    static List<HgKvEntry> toList(List<Kv> kvList) {
        if (kvList == null || kvList.isEmpty()) {
            return HgStoreClientConst.EMPTY_LIST;
        }

        Iterator<Kv> iter = kvList.iterator();
        List<HgKvEntry> resList = new ArrayList<>(kvList.size());

        while (iter.hasNext()) {
            Kv entry = iter.next();
            resList.add(new GrpcKvEntryImpl(entry.getKey().toByteArray(),
                                            entry.getValue().toByteArray(), entry.getCode()));
        }

        return resList;
    }

    static StatusRuntimeException toErr(String msg) {
        return new StatusRuntimeException(Status.UNKNOWN.withDescription(msg));
    }

    static ByteString toBs(byte[] bytes) {
        return ByteString.copyFrom((bytes != null) ? bytes : HgStoreClientConst.EMPTY_BYTES);
    }
}
