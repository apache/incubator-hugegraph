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

import org.apache.hugegraph.store.HgOwnerKey;
import org.apache.hugegraph.store.client.HgStoreNodeSession;
import org.apache.hugegraph.store.client.util.HgStoreClientConst;
import org.apache.hugegraph.store.grpc.common.Header;

import com.google.protobuf.ByteString;

public class ScanUtil {


    public static Header getHeader(HgStoreNodeSession nodeSession) {
        return Header.newBuilder().setGraph(nodeSession.getGraphName()).build();
    }

    public static HgOwnerKey toOk(HgOwnerKey key) {
        return key == null ? HgStoreClientConst.EMPTY_OWNER_KEY : key;
    }

    public static ByteString toBs(byte[] bytes) {
        return ByteString.copyFrom((bytes != null) ? bytes : HgStoreClientConst.EMPTY_BYTES);
    }

    public static ByteString getHgOwnerKey(HgOwnerKey ownerKey) {
        return toBs(toOk(ownerKey).getKey());
    }

    public static byte[] getQuery(byte[] query) {
        return query != null ? query : HgStoreClientConst.EMPTY_BYTES;
    }

    public static long getLimit(long limit) {
        return limit <= HgStoreClientConst.NO_LIMIT ? Integer.MAX_VALUE : limit;
    }
}
