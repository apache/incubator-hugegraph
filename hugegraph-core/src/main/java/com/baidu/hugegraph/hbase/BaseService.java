/*
 * Copyright (C) 2017 Baidu, Inc. All Rights Reserved.
 */
package com.baidu.hugegraph.hbase;

import java.io.IOException;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.tinkerpop.gremlin.structure.T;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.baidu.hugegraph.structure.HugeElement;
import com.baidu.hugegraph.structure.HugeGraph;
import com.baidu.hugegraph.structure.HugeGraphConfiguration;
import com.baidu.hugegraph.utils.Constants;
import com.baidu.hugegraph.utils.HugeGraphUtils;
import com.baidu.hugegraph.utils.ValueUtils;
import com.google.common.annotations.VisibleForTesting;

/**
 * Created by zhangsuochao on 17/2/7.
 */
public class BaseService {

    private static final Logger logger = LoggerFactory.getLogger(BaseService.class);
    protected final HugeGraph graph;
    protected Table table;
    protected Connection connection;

    public BaseService(HugeGraph graph, String tableName) {
        this.graph = graph;
        init(tableName);
    }

    /**
     * Init hbase connection
     */
    protected void init(String tableName) {
        HugeGraphConfiguration conf = (HugeGraphConfiguration) graph.configuration();
        this.connection = HugeGraphUtils.getConnection(conf);
        try {
            String ns = conf.getGraphNamespace();
            TableName tn = TableName.valueOf(ns, tableName);
            this.table = connection.getTable(tn);
        } catch (IOException e) {
            e.printStackTrace();
            logger.error(e.getMessage());
        }

    }

    protected Scan getPropertyScan(String label) {
        Scan scan = new Scan();
        SingleColumnValueFilter valueFilter = new SingleColumnValueFilter(Constants.DEFAULT_FAMILY_BYTES,
                Constants.LABEL_BYTES, CompareFilter.CompareOp.EQUAL,
                new BinaryComparator(ValueUtils.serialize(label)));
        valueFilter.setFilterIfMissing(true);
        scan.setFilter(valueFilter);
        return scan;
    }

    protected Scan getPropertyScan(String label, byte[] key, byte[] val) {
        Scan scan = new Scan();
        SingleColumnValueFilter labelFilter = new SingleColumnValueFilter(Constants.DEFAULT_FAMILY_BYTES,
                Constants.LABEL_BYTES, CompareFilter.CompareOp.EQUAL,
                new BinaryComparator(ValueUtils.serialize(label)));
        labelFilter.setFilterIfMissing(true);
        SingleColumnValueFilter valueFilter = new SingleColumnValueFilter(Constants.DEFAULT_FAMILY_BYTES,
                key, CompareFilter.CompareOp.EQUAL, new BinaryComparator(val));
        valueFilter.setFilterIfMissing(true);
        FilterList filterList = new FilterList(labelFilter, valueFilter);
        scan.setFilter(filterList);
        return scan;
    }

    /**
     * @param element
     * @param keyValues
     */
    public void updateProperty(HugeElement element, Object... keyValues) {

        Put put = new Put(ValueUtils.serializeWithSalt(element.id()));
        for (int i = 0; i < keyValues.length; i = i + 2) {
            if (!keyValues[i].equals(T.id) && !keyValues[i].equals(T.label)) {
                byte[] keyBytes = Bytes.toBytes((String) keyValues[i]);
                byte[] valueBytes = ValueUtils.serialize(keyValues[i + 1]);
                put.addColumn(Constants.DEFAULT_FAMILY_BYTES, keyBytes, valueBytes);
            }

        }

        put.addColumn(Constants.DEFAULT_FAMILY_BYTES, Constants.UPDATED_AT_BYTES,
                ValueUtils.serialize(element.getUpdatedAt()));
        try {
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @VisibleForTesting
    public void close(boolean clear) {
        if (clear) {
            clear();
        }
        try {
            table.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void clear() {
        ResultScanner scanner = null;
        try {
            scanner = table.getScanner(new Scan());
            scanner.iterator().forEachRemaining(result -> {
                try {
                    table.delete(new Delete(result.getRow()));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });

            logger.info("###Clear table {}", table.getName().getNameWithNamespaceInclAsString());
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (scanner != null) {
                scanner.close();
            }
        }
    }
}
