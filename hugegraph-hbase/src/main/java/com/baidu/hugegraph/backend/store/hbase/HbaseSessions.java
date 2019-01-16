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

package com.baidu.hugegraph.backend.store.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Future;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.RegionMetrics;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotEnabledException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter.RowRange;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.VersionInfo;

import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.serializer.BinarySerializer;
import com.baidu.hugegraph.backend.store.BackendEntry.BackendColumn;
import com.baidu.hugegraph.backend.store.BackendEntry.BackendIterator;
import com.baidu.hugegraph.backend.store.BackendSession;
import com.baidu.hugegraph.backend.store.BackendSessionPool;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.util.Bytes;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.StringEncoding;
import com.baidu.hugegraph.util.VersionUtil;

public class HbaseSessions extends BackendSessionPool {

    private final String namespace;
    private Connection hbase;

    public HbaseSessions(String namespace, String store) {
        super(namespace + "/" + store);
        this.namespace = namespace;
    }

    private Table table(String table) throws IOException {
        E.checkState(this.hbase != null, "HBase connection is not opened");
        TableName tableName = TableName.valueOf(this.namespace, table);
        return this.hbase.getTable(tableName);
    }

    @Override
    public synchronized void open(HugeConfig conf) throws IOException {
        String hosts = conf.get(HbaseOptions.HBASE_HOSTS);
        int port = conf.get(HbaseOptions.HBASE_PORT);
        String znodeParent = conf.get(HbaseOptions.HBASE_ZNODE_PARENT);

        Configuration config = HBaseConfiguration.create();
        config.set(HConstants.ZOOKEEPER_QUORUM, hosts);
        config.set(HConstants.ZOOKEEPER_CLIENT_PORT, String.valueOf(port));
        config.set(HConstants.ZOOKEEPER_ZNODE_PARENT, znodeParent);
        // Set hbase.hconnection.threads.max 64 to avoid OOM(default value: 256)
        config.setInt("hbase.hconnection.threads.max",
                      conf.get(HbaseOptions.HBASE_THREADS_MAX));

        this.hbase = ConnectionFactory.createConnection(config);
    }

    @Override
    protected synchronized boolean opened() {
        return this.hbase != null && !this.hbase.isClosed();
    }

    @Override
    public final Session session() {
        return (Session) super.getOrNewSession();
    }

    @Override
    protected Session newSession() {
        return new Session();
    }

    @Override
    protected synchronized void doClose() {
        if (this.hbase == null || this.hbase.isClosed()) {
            return;
        }
        try {
            this.hbase.close();
        } catch (IOException e) {
            throw new BackendException("Failed to close HBase connection", e);
        }
    }

    public boolean existsNamespace() throws IOException {
        try(Admin admin = this.hbase.getAdmin()) {
            for (NamespaceDescriptor ns : admin.listNamespaceDescriptors()) {
                if (this.namespace.equals(ns.getName())) {
                    return true;
                }
            }
        }
        return false;
    }

    public void createNamespace() throws IOException {
        NamespaceDescriptor ns = NamespaceDescriptor.create(this.namespace)
                                                    .build();
        try(Admin admin = this.hbase.getAdmin()) {
            admin.createNamespace(ns);
        }
    }

    public void dropNamespace() throws IOException {
        try(Admin admin = this.hbase.getAdmin()) {
            admin.deleteNamespace(this.namespace);
        }
    }

    public boolean existsTable(String table) throws IOException {
        TableName tableName = TableName.valueOf(this.namespace, table);
        try(Admin admin = this.hbase.getAdmin()) {
            return admin.tableExists(tableName);
        }
    }

    public void createTable(String table, List<byte[]> cfs) throws IOException {
        TableDescriptorBuilder tb = TableDescriptorBuilder.newBuilder(
                                    TableName.valueOf(this.namespace, table));
        for (byte[] cf : cfs) {
            tb.setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(cf)
                                                            .build());
        }
        try(Admin admin = this.hbase.getAdmin()) {
            admin.createTable(tb.build());
        }
    }

    public void dropTable(String table) throws IOException {
        TableName tableName = TableName.valueOf(this.namespace, table);
        try(Admin admin = this.hbase.getAdmin()) {
            try {
                admin.disableTable(tableName);
            } catch (TableNotEnabledException ignored) {
                // pass
            }
            admin.deleteTable(tableName);
        }
    }

    public Future<Void> truncateTable(String table) throws IOException {
        assert this.existsTable(table);
        TableName tableName = TableName.valueOf(this.namespace, table);
        try(Admin admin = this.hbase.getAdmin()) {
            try {
                admin.disableTable(tableName);
            } catch (TableNotEnabledException ignored) {
                // pass
            }
            return admin.truncateTableAsync(tableName, false);
        }
    }

    public long storeSize(String table) throws IOException {
        long total = 0;
        try(Admin admin = this.hbase.getAdmin()) {
            for (ServerName rs : admin.getRegionServers()) {
                // NOTE: we can use getLoad() before hbase 2.0
                //ServerLoad load = admin.getClusterStatus().getLoad(rs);
                //total += load.getStorefileSizeMB() * Bytes.MB;
                //total += load.getMemStoreSizeMB() * Bytes.MB;
                TableName tableName = TableName.valueOf(this.namespace, table);
                for (RegionMetrics m : admin.getRegionMetrics(rs, tableName)) {
                    total += m.getStoreFileSize().getLongValue();
                    total += m.getMemStoreSize().getLongValue();
                }
            }
        }
        return total;
    }

    /**
     * Session for HBase
     */
    public final class Session extends BackendSession {

        private boolean closed;
        private final Map<String, List<Row>> batch;

        public Session() {
            this.closed = false;
            this.batch = new HashMap<>();
        }

        private void batch(String table, Row row) {
            List<Row> rows = this.batch.get(table);
            if (rows == null) {
                rows = new ArrayList<>();
                this.batch.put(table, rows);
            }
            rows.add(row);
        }

        private int batchSize() {
            int size = 0;
            for (List<Row> puts : this.batch.values()) {
                size += puts.size();
            }
            return size;
        }

        @Override
        public void close() {
            assert this.closeable();
            this.closed = true;
        }

        @Override
        public boolean closed() {
            return this.closed;
        }

        /**
         * Any change in the session
         */
        @Override
        public boolean hasChanges() {
            return this.batch.size() > 0;
        }

        /**
         * Commit all updates(put/delete) to DB
         */
        @Override
        public Integer commit() {
            int count = this.batchSize();
            if (count <= 0) {
                return 0;
            }

            // TODO: this will not be atomic, to be improved
            for (Entry<String, List<Row>> action : this.batch.entrySet()) {
                List<Row> rows = action.getValue();
                Object[] results = new Object[rows.size()];
                try (Table table = table(action.getKey())) {
                    table.batch(rows, results);
                } catch (InterruptedException e) {
                    // Try again
                    continue;
                } catch (IOException e) {
                    // TODO: Mark and delete committed records
                    throw new BackendException(e);
                }
            }

            // Clear batch if write() successfully (retained if failed)
            this.batch.clear();

            return count;
        }

        /**
         * Rollback all updates(put/delete) not committed
         */
        @Override
        public void rollback() {
            this.batch.clear();
        }

        /**
         * Add a row record to a table
         */
        public void put(String table, byte[] family, byte[] rowkey,
                        Collection<BackendColumn> columns) {
            Put put = new Put(rowkey);
            for (BackendColumn column : columns) {
                put.addColumn(family, column.name, column.value);
            }
            this.batch(table, put);
        }

        /**
         * Add a row record to a table(for index)
         */
        public void put(String table, byte[] family,
                        byte[] rowkey, byte[] value) {
            Put put = new Put(rowkey);
            put.addColumn(family, BinarySerializer.EMPTY_BYTES, value);
            this.batch(table, put);
        }

        /**
         * Delete a record by rowkey and qualifier from a table
         */
        public void remove(String table, byte[] family,
                           byte[] rowkey, byte[] qualifier) {
            this.remove(table, family, rowkey, qualifier, false);
        }

        /**
         * Delete a record by rowkey and qualifier from a table,
         * just delete the latest version of the specified column if need
         */
        public void remove(String table, byte[] family, byte[] rowkey,
                           byte[] qualifier, boolean latestVersion) {
            assert family != null;
            assert rowkey != null;
            E.checkArgument(qualifier != null,
                            "HBase qualifier can't be null when removing");
            Delete delete = new Delete(rowkey);
            if (latestVersion) {
                // Just delete the latest version of the specified column
                delete.addColumn(family, qualifier);
            } else {
                // Delete all versions of the specified column
                delete.addColumns(family, qualifier);
            }
            this.batch(table, delete);
        }

        /**
         * Delete a record by rowkey from a table
         */
        public void delete(String table, byte[] family, byte[] rowkey) {
            assert rowkey != null;
            Delete delete = new Delete(rowkey);
            if (family != null) {
                delete.addFamily(family);
            }
            this.batch(table, delete);
        }

        /**
         * Get a record by rowkey and qualifier from a table
         */
        public RowIterator get(String table, byte[] family,
                               byte[] rowkey, byte[] qualifier) {
            assert !this.hasChanges();

            Get get = new Get(rowkey);
            get.addColumn(family, qualifier);

            try (Table htable = table(table)) {
                return new RowIterator(htable.get(get));
            } catch (IOException e) {
                throw new BackendException(e);
            }
        }

        /**
         * Get a record by rowkey from a table
         */
        public RowIterator get(String table, byte[] family, byte[] rowkey) {
            assert !this.hasChanges();

            Get get = new Get(rowkey);
            if (family != null) {
                get.addFamily(family);
            }

            try (Table htable = table(table)) {
                return new RowIterator(htable.get(get));
            } catch (IOException e) {
                throw new BackendException(e);
            }
        }

        /**
         * Get multi records by rowkeys from a table
         */
        public RowIterator get(String table, byte[] family,
                               Set<byte[]> rowkeys) {
            assert !this.hasChanges();

            List<Get> gets = new ArrayList<>(rowkeys.size());
            for (byte[] rowkey : rowkeys) {
                Get get = new Get(rowkey);
                if (family != null) {
                    get.addFamily(family);
                }
                gets.add(get);
            }

            try (Table htable = table(table)) {
                return new RowIterator(htable.get(gets));
            } catch (IOException e) {
                throw new BackendException(e);
            }
        }

        /**
         * Scan all records from a table
         */
        public RowIterator scan(String table, long limit) {
            assert !this.hasChanges();
            Scan scan = new Scan();
            if (limit >= 0) {
                scan.setFilter(new PageFilter(limit));
            }
            return this.scan(table, scan);
        }

        /**
         * Scan records by rowkey prefix from a table
         */
        public RowIterator scan(String table, byte[] prefix) {
            assert !this.hasChanges();
            return this.scan(table, prefix, true, prefix);
        }

        /**
         * Scan records by multi rowkey prefixs from a table
         */
        public RowIterator scan(String table, Set<byte[]> prefixs) {
            assert !this.hasChanges();

            FilterList orFilters = new FilterList(Operator.MUST_PASS_ONE);
            for (byte[] prefix : prefixs) {
                FilterList andFilters = new FilterList(Operator.MUST_PASS_ALL);
                List<RowRange> ranges = new ArrayList<>();
                ranges.add(new RowRange(prefix, true, null, true));
                andFilters.addFilter(new MultiRowRangeFilter(ranges));
                andFilters.addFilter(new PrefixFilter(prefix));

                orFilters.addFilter(andFilters);
            }

            Scan scan = new Scan().setFilter(orFilters);
            return this.scan(table, scan);
        }

        /**
         * Scan records by rowkey start and prefix from a table
         */
        public RowIterator scan(String table, byte[] startRow,
                                boolean inclusiveStart, byte[] prefix) {
            assert !this.hasChanges();
            Scan scan = new Scan().withStartRow(startRow, inclusiveStart)
                                  .setFilter(new PrefixFilter(prefix));
            return this.scan(table, scan);
        }

        /**
         * Scan records by rowkey range from a table
         */
        public RowIterator scan(String table, byte[] startRow, byte[] stopRow) {
            assert !this.hasChanges();
            return this.scan(table, startRow, true, stopRow, false);
        }

        /**
         * Scan records by rowkey range from a table
         */
        public RowIterator scan(String table,
                                byte[] startRow, boolean inclusiveStart,
                                byte[] stopRow, boolean inclusiveStop) {
            assert !this.hasChanges();
            Scan scan = new Scan().withStartRow(startRow, inclusiveStart);
            if (stopRow != null) {
                String version = VersionInfo.getVersion();
                if (inclusiveStop && !VersionUtil.gte(version, "2.0")) {
                    // The parameter stoprow-inclusive doesn't work before v2.0
                    // https://issues.apache.org/jira/browse/HBASE-20675
                    inclusiveStop = false;
                    // Add a trailing 0 byte to stopRow
                    stopRow = Arrays.copyOf(stopRow, stopRow.length + 1);
                }
                if (Bytes.equals(startRow, stopRow) &&
                    inclusiveStart && !inclusiveStop) {
                    // Bug https://issues.apache.org/jira/browse/HBASE-21618
                    return new RowIterator();
                }
                scan.withStopRow(stopRow, inclusiveStop);
            }
            return this.scan(table, scan);
        }

        /**
         * Inner scan: send scan request to HBase and get iterator
         */
        private RowIterator scan(String table, Scan scan) {
            try (Table htable = table(table)) {
                return new RowIterator(htable.getScanner(scan));
            } catch (IOException e) {
                throw new BackendException(e);
            }
        }

        /**
         * Increase a counter by rowkey and qualifier to a table
         */
        public long increase(String table, byte[] family, byte[] rowkey,
                             byte[] qualifier, long value) {
            try (Table htable = table(table)) {
                return htable.incrementColumnValue(rowkey, family,
                                                   qualifier, value);
            } catch (IOException e) {
                throw new BackendException(e);
            }
        }

        /**
         * Get store size of specified table
         */
        public long storeSize(String table) throws IOException {
            return HbaseSessions.this.storeSize(table);
        }

        /**
         * Just for debug
         */
        @SuppressWarnings("unused")
        private void dump(String table, Scan scan) throws IOException {
            System.out.println(String.format(">>>> scan table %s with %s",
                                             table, scan));
            RowIterator iterator = this.scan(table, scan);
            while (iterator.hasNext()) {
                Result row = iterator.next();
                System.out.println(StringEncoding.format(row.getRow()));
                CellScanner cellScanner = row.cellScanner();
                while (cellScanner.advance()) {
                    Cell cell = cellScanner.current();
                    byte[] key = CellUtil.cloneQualifier(cell);
                    byte[] val = CellUtil.cloneValue(cell);
                    System.out.println(String.format("  %s=%s",
                                       StringEncoding.format(key),
                                       StringEncoding.format(val)));
                }
            }
        }
    }

    protected static class RowIterator implements BackendIterator<Result> {

        private final ResultScanner resultScanner;
        private final Iterator<Result> results;

        private byte[] position = null;

        public RowIterator(ResultScanner resultScanner) {
            this.resultScanner = resultScanner;
            this.results = resultScanner.iterator();
        }

        public RowIterator(Result... results) {
            this.resultScanner = null;
            List<Result> rs = new ArrayList<>(results.length);
            for (Result result : results) {
                // Get by Ids may return empty result
                if (!result.isEmpty()) {
                    rs.add(result);
                }
            }
            this.results = rs.iterator();
        }

        @Override
        public boolean hasNext() {
            boolean has = this.results.hasNext();
            if (!has) {
                this.position = null;
                this.close();
            }
            return has;
        }

        @Override
        public Result next() {
            // Reset position due to results.next() may throw ex
            this.position = null;

            Result next = this.results.next();

            // Update position for paging
            this.position = next.getRow();

            return next;
        }

        @Override
        public void close() {
            if (this.resultScanner != null) {
                this.resultScanner.close();
            }
        }

        @Override
        public byte[] position() {
            return this.position;
        }
    }
}
