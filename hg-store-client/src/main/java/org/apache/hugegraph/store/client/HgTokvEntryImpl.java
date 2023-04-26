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

package org.apache.hugegraph.store.client;

import java.util.Arrays;
import java.util.Objects;

import org.apache.hugegraph.store.HgOwnerKey;
import org.apache.hugegraph.store.HgTokvEntry;

/**
 * created on 2021/10/14
 */
class HgTokvEntryImpl implements HgTokvEntry {

    private final String table;
    private final HgOwnerKey ownerKey;
    private final byte[] value;

    HgTokvEntryImpl(String table, HgOwnerKey ownerKey, byte[] value) {
        this.table = table;
        this.ownerKey = ownerKey;
        this.value = value;
    }

    @Override
    public String table() {
        return this.table;
    }

    @Override
    public HgOwnerKey ownerKey() {
        return this.ownerKey;
    }

    @Override
    public byte[] value() {
        return this.value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        HgTokvEntryImpl that = (HgTokvEntryImpl) o;
        return Objects.equals(table, that.table) && Objects.equals(ownerKey, that.ownerKey) &&
               Arrays.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(table, ownerKey);
        result = 31 * result + Arrays.hashCode(value);
        return result;
    }

    @Override
    public String toString() {
        return "HgTokvEntryImpl{" +
               "table='" + table + '\'' +
               ", okv=" + ownerKey +
               ", value=" + Arrays.toString(value) +
               '}';
    }
}
