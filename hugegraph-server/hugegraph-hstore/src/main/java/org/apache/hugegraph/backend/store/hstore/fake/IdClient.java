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

package org.apache.hugegraph.backend.store.hstore.fake;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.apache.hugegraph.backend.store.hstore.HstoreSessions;
import org.apache.hugegraph.pd.common.Useless;
import org.apache.hugegraph.pd.grpc.Pdpb;

@Useless
public abstract class IdClient {

    protected HstoreSessions.Session session;
    protected String table;

    public IdClient(HstoreSessions.Session session, String table) {
        this.session = session;
        this.table = table;
    }

    protected static byte[] b(long value) {
        return ByteBuffer.allocate(Long.BYTES).order(
                ByteOrder.nativeOrder()).putLong(value).array();
    }

    protected static long l(byte[] bytes) {
        assert bytes.length == Long.BYTES;
        return ByteBuffer.wrap(bytes).order(
                ByteOrder.nativeOrder()).getLong();
    }

    public abstract Pdpb.GetIdResponse getIdByKey(String key, int delta)
            throws Exception;

    public abstract Pdpb.ResetIdResponse resetIdByKey(String key) throws Exception;

    public abstract void increaseId(String key, long increment)
            throws Exception;
}
