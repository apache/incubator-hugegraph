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

package com.baidu.hugegraph.backend.id;

import com.baidu.hugegraph.structure.HugeVertex;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.NumericUtil;
import com.baidu.hugegraph.util.StringEncoding;

public abstract class IdGenerator {

    public abstract Id generate(HugeVertex vertex);

    public static Id of(String id) {
        return new StringId(id);
    }

    public static Id of(long id) {
        return new LongId(id);
    }

    public static Id of(byte[] bytes, boolean number) {
        return number ? new LongId(bytes) : new StringId(bytes);
    }

    /**
     * Generate a string id
     */
    public Id generate(String id) {
        return of(id);
    }

    /**
     * Generate a long id
     */
    public Id generate(long id) {
        return of(id);
    }

    /****************************** id defines ******************************/

    public static final class StringId implements Id {

        private final String id;

        public StringId(String id) {
            E.checkArgument(!id.isEmpty(), "The id can't be empty");
            this.id = id;
        }

        public StringId(byte[] bytes) {
            this.id = StringEncoding.decode(bytes);
        }

        @Override
        public boolean number() {
            return false;
        }

        @Override
        public Object asObject() {
            return this.id;
        }

        @Override
        public String asString() {
            return this.id;
        }

        @Override
        public long asLong() {
            return Long.parseLong(this.id);
        }

        @Override
        public byte[] asBytes() {
            return StringEncoding.encode(this.id);
        }

        @Override
        public int length() {
            return this.id.length();
        }

        @Override
        public int compareTo(Id other) {
            return this.id.compareTo(other.asString());
        }

        @Override
        public int hashCode() {
            return this.id.hashCode();
        }

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof StringId)) {
                return false;
            }
            return this.id.equals(((StringId) other).id);
        }

        @Override
        public String toString() {
            return this.id;
        }
    }

    public static final class LongId extends Number implements Id {

        private static final long serialVersionUID = -7732461469037400190L;

        private final long id;

        public LongId(long id) {
            this.id = id;
        }

        public LongId(byte[] bytes) {
            this.id = NumericUtil.bytesToLong(bytes);
        }

        @Override
        public boolean number() {
            return true;
        }

        @Override
        public Object asObject() {
            return this.id;
        }

        @Override
        public String asString() {
            // TODO: encode with base64
            return String.valueOf(this.id);
        }

        @Override
        public long asLong() {
            return this.id;
        }

        @Override
        public byte[] asBytes() {
            return NumericUtil.longToBytes(this.id);
        }

        @Override
        public int length() {
            return Long.BYTES;
        }

        @Override
        public int compareTo(Id other) {
            return Long.compare(this.id, other.asLong());
        }

        @Override
        public int hashCode() {
            return Long.hashCode(this.id);
        }

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof LongId)) {
                return false;
            }
            return this.id == ((LongId) other).id;
        }

        @Override
        public String toString() {
            return String.valueOf(this.id);
        }

        @Override
        public int intValue() {
            return (int) this.id;
        }

        @Override
        public long longValue() {
            return this.id;
        }

        @Override
        public float floatValue() {
            return this.id;
        }

        @Override
        public double doubleValue() {
            return this.id;
        }
    }
}
