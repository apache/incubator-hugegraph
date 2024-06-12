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

import java.util.ArrayList;
import java.util.List;

import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

public class RocksdbDump {

    // @Test
    public void dump() throws RocksDBException {
        String dbPath =
                "D:\\Workspaces\\baidu\\hugegraph\\hugegraph-store\\tmp\\8500\\db\\default" +
                "\\hugegraph\\g";
        List<ColumnFamilyDescriptor> cfDescriptors = new ArrayList<>();
        List<byte[]> columnFamilyBytes = RocksDB.listColumnFamilies(new Options(), dbPath);
        ColumnFamilyOptions cfOptions = new ColumnFamilyOptions();
        if (columnFamilyBytes.size() > 0) {
            for (byte[] columnFamilyByte : columnFamilyBytes) {
                cfDescriptors.add(new ColumnFamilyDescriptor(columnFamilyByte, cfOptions));
            }
        }

        final List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();
        try (final DBOptions options = new DBOptions();
             final RocksDB db = RocksDB.open(options, dbPath, cfDescriptors, columnFamilyHandles)) {
            for (ColumnFamilyHandle handle : columnFamilyHandles) {
                System.out.println(new String(handle.getName()) + "---------------");
                try (RocksIterator iterator = db.newIterator(handle, new ReadOptions())) {
                    iterator.seekToFirst();
                    while (iterator.isValid()) {
                        byte[] key = iterator.key();
                        // System.out.println(new String(key) + " -- " + Bytes.toHex(key));
                        iterator.next();
                    }
                }
            }
        }
    }
}
