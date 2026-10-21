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

package org.apache.hugegraph.pd.core.store;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;

/**
 * Holds a RocksDB LOCK from a separate JVM, the way a previous PD process does while it is
 * still stopping. Prints {@link #LOCKED} once the database is open, then releases it after
 * the given number of milliseconds, or holds it until killed when the number is negative.
 */
public final class RocksDBLockHolder {

    public static final String LOCKED = "LOCKED";

    private RocksDBLockHolder() {
    }

    public static void main(String[] args) throws Exception {
        String dbPath = args[0];
        long holdMs = Long.parseLong(args[1]);
        RocksDB.loadLibrary();
        try (Options options = new Options().setCreateIfMissing(true);
             RocksDB ignored = RocksDB.open(options, dbPath)) {
            System.out.println(LOCKED);
            System.out.flush();
            Thread.sleep(holdMs < 0 ? Long.MAX_VALUE : holdMs);
        }
    }
}
