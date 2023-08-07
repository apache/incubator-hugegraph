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

import java.util.Arrays;

import org.apache.hugegraph.store.HgKvEntry;


class GrpcKvEntryImpl implements HgKvEntry {
    private final byte[] key;
    private final byte[] value;
    private final int code;

    GrpcKvEntryImpl(byte[] k, byte[] v, int code) {
        this.key = k;
        this.value = v;
        this.code = code;
    }

    @Override
    public int code() {
        return code;
    }

    @Override
    public byte[] key() {
        return key;
    }

    @Override
    public byte[] value() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        GrpcKvEntryImpl hgKvEntry = (GrpcKvEntryImpl) o;
        return Arrays.equals(key, hgKvEntry.key) && Arrays.equals(value, hgKvEntry.value);
    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(key);
        result = 31 * result + Arrays.hashCode(value);
        return result;
    }

    @Override
    public String toString() {
        return "HgKvEntryImpl{" +
               "key=" + Arrays.toString(key) +
               ", value=" + Arrays.toString(value) +
               ", code=" + code +
               '}';
    }
}