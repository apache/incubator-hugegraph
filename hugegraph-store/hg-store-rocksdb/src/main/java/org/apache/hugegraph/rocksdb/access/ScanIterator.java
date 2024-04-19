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

package org.apache.hugegraph.rocksdb.access;

import java.io.Closeable;

public interface ScanIterator extends Closeable {

    boolean hasNext();

    boolean isValid();

    <T> T next();

    default long count() {
        return 0;
    }

    default byte[] position() {
        return new byte[0];
    }

    default void seek(byte[] position) {
    }

    @Override
    void close();

    abstract class Trait {

        public static final int SCAN_ANY = 0x80;
        public static final int SCAN_PREFIX_BEGIN = 0x01;
        public static final int SCAN_PREFIX_END = 0x02;
        public static final int SCAN_GT_BEGIN = 0x04;
        public static final int SCAN_GTE_BEGIN = 0x0c;
        public static final int SCAN_LT_END = 0x10;
        public static final int SCAN_LTE_END = 0x30;
        public static final int SCAN_KEYONLY = 0x40;
        public static final int SCAN_HASHCODE = 0x100;
    }
}
