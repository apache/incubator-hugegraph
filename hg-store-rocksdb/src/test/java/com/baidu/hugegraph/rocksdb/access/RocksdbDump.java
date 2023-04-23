package com.baidu.hugegraph.rocksdb.access;

import com.baidu.hugegraph.util.Bytes;
// import org.junit.Test;
import org.rocksdb.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class RocksdbDump {

    // @Test
    public void dump() throws RocksDBException {
        String dbPath = "D:\\Workspaces\\baidu\\hugegraph\\hugegraph-store\\tmp\\8500\\db\\default\\hugegraph\\g";
        List<ColumnFamilyDescriptor> cfDescriptors = new ArrayList<ColumnFamilyDescriptor>();
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
