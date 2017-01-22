/*
 * Copyright (C) 2017 Baidu, Inc. All Rights Reserved.
 */
package com.baidu.hugegraph.utils;

import static com.baidu.hugegraph.utils.Constants.DEFAULT_FAMILY;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.NamespaceNotFoundException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;

import com.baidu.hugegraph.structure.HugeEdge;
import com.baidu.hugegraph.structure.HugeElement;
import com.baidu.hugegraph.structure.HugeGraph;
import com.baidu.hugegraph.structure.HugeGraphConfiguration;
import com.baidu.hugegraph.structure.HugeVertex;

/**
 * Created by zhangsuochao on 17/2/8.
 */
public class HugeGraphUtils {

    private static final Map<String, Connection> connections = new ConcurrentHashMap<>();

    public static Object generateIdIfNeeded(Object id) {
        if (id == null) {
            id = UUID.randomUUID().toString();
        } else if (id instanceof Integer) {
            id = ((Integer) id).longValue();
        } else if (id instanceof Number) {
            id = ((Number) id).longValue();
        }
        return id;
    }

    /**
     * @param scanner
     * @param graph
     *
     * @return
     *
     * @throws IOException
     */
    public static Iterator<Vertex> parseVertexScanner(ResultScanner scanner, HugeGraph
            graph)
            throws IOException {
        Result result;
        HugeVertex element;
        List<Vertex> lst = new ArrayList<>();
        while ((result = scanner.next()) != null) {
            element = (HugeVertex) parseResult(HugeElement.ElementType.VERTEX, result, graph);
            if (element != null) {
                lst.add(element);
            }
        }
        return lst.iterator();
    }

    /**
     * @param scanner
     * @param graph
     *
     * @return
     *
     * @throws IOException
     */
    public static Iterator<Edge> parseEdgeScanner(ResultScanner scanner, HugeGraph
            graph) throws IOException {
        Result result;
        HugeEdge element;
        List<Edge> lst = new ArrayList<>();
        while ((result = scanner.next()) != null) {
            element = (HugeEdge) parseResult(HugeElement.ElementType.EDGE, result, graph);
            if (element != null) {
                lst.add(element);
            }
        }
        return lst.iterator();
    }

    /**
     * @param result
     * @param graph
     *
     * @return
     */
    public static HugeElement parseResult(HugeElement.ElementType elementType, Result result, HugeGraph graph) {
        if (result.isEmpty()) {
            return null;
        }

        Object id = ValueUtils.deserializeWithSalt(result.getRow());
        String label = null;
        Long createdAt = null;
        Long updatedAt = null;
        Map<String, Object> rawProps = new HashMap<>();
        for (Cell cell : result.listCells()) {
            String key = Bytes.toString(CellUtil.cloneQualifier(cell));
            if (!Graph.Hidden.isHidden(key)) {
                rawProps.put(key, ValueUtils.deserialize(CellUtil.cloneValue(cell)));
            } else if (key.equals(Constants.LABEL)) {
                label = ValueUtils.deserialize(CellUtil.cloneValue(cell));
            } else if (key.equals(Constants.CREATED_AT)) {
                createdAt = ValueUtils.deserialize(CellUtil.cloneValue(cell));
            } else if (key.equals(Constants.UPDATED_AT)) {
                updatedAt = ValueUtils.deserialize(CellUtil.cloneValue(cell));
            }
        }

        HugeElement element = null;
        if (HugeElement.ElementType.VERTEX.equals(elementType)) {
            element = new HugeVertex(graph, id, label);

        } else if (HugeElement.ElementType.EDGE.equals(elementType)) {
            Object outVertexId = ValueUtils.deserialize(result.getValue(Constants.DEFAULT_FAMILY_BYTES, Constants
                    .FROM_BYTES));
            Object inVertexId = ValueUtils.deserialize(result.getValue(Constants.DEFAULT_FAMILY_BYTES, Constants
                    .TO_BYTES));
            element = new HugeEdge(graph, id, label, new HugeVertex(graph, inVertexId, null), new HugeVertex(graph,
                    outVertexId, null));
        }

        element.setCreatedAt(createdAt);
        element.setUpdatedAt(updatedAt);
        element.setPropertyMap(rawProps);

        return element;
    }

    public static Map<String, Object> propertiesToMap(Object... keyValues) {
        Map<String, Object> props = new HashMap<>();
        for (int i = 0; i < keyValues.length; i = i + 2) {
            Object key = keyValues[i];
            if (key.equals(T.id) || key.equals(T.label)) {
                continue;
            }
            String keyStr = key.toString();
            Object value = keyValues[i + 1];
            ElementHelper.validateProperty(keyStr, value);
            props.put(keyStr, value);
        }
        return props;
    }

    public static void createTables(HugeGraphConfiguration configuration) {
        Admin admin = null;

        try {
            Connection connection = getConnection(configuration);
            admin = connection.getAdmin();
            createNamespace(configuration, admin);
            createTable(configuration, admin, Constants.EDGES, Constants.FOREVER);
            createTable(configuration, admin, Constants.VERTICES, Constants.FOREVER);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (admin != null) {
                    admin.close();
                }
            } catch (IOException ignored) {
                // TODO
            }
        }

    }

    private static void createNamespace(HugeGraphConfiguration config, Admin admin) throws IOException {
        String name = config.getGraphNamespace();
        try {
            NamespaceDescriptor ns = admin.getNamespaceDescriptor(name);
        } catch (NamespaceNotFoundException e) {
            admin.createNamespace(NamespaceDescriptor.create(name).build());
        }
    }

    private static void createTable(HugeGraphConfiguration config, Admin admin, String name, int ttl)
            throws IOException {
        String ns = config.getGraphNamespace();
        TableName tableName = TableName.valueOf(ns, name);
        if (admin.tableExists(tableName)) {
            return;
        }
        HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
        HColumnDescriptor columnDescriptor = new HColumnDescriptor(DEFAULT_FAMILY)
                .setBloomFilterType(BloomType.ROW)
                .setDataBlockEncoding(DataBlockEncoding.FAST_DIFF)
                .setMaxVersions(1)
                .setMinVersions(0)
                .setBlocksize(32768)
                .setBlockCacheEnabled(true)
                .setTimeToLive(ttl);
        tableDescriptor.addFamily(columnDescriptor);
        admin.createTable(tableDescriptor);
    }

    public static Connection getConnection(HugeGraphConfiguration config) {
        Connection conn = connections.get(config.getGraphNamespace());
        if (conn != null) {
            return conn;
        }

        Configuration hbaseConf = HBaseConfiguration.create();
        hbaseConf.set(HugeGraphConfiguration.Keys.ZOOKEEPER_QUORUM,
                config.getString(HugeGraphConfiguration.Keys.ZOOKEEPER_QUORUM));
        hbaseConf.set(HugeGraphConfiguration.Keys.ZOOKEEPER_CLIENTPORT, config.getString
                (HugeGraphConfiguration.Keys.ZOOKEEPER_CLIENTPORT));
        try {
            conn = ConnectionFactory.createConnection(hbaseConf);
        } catch (IOException e) {
            e.printStackTrace();
        }

        if (conn != null) {
            connections.put(config.getGraphNamespace(), conn);
        }

        return conn;
    }

}
