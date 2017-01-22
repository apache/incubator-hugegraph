/*
 * Copyright (C) 2017 Baidu, Inc. All Rights Reserved.
 */
package com.baidu.hugegraph.hbase;

import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.baidu.hugegraph.structure.HugeElement;
import com.baidu.hugegraph.structure.HugeGraph;
import com.baidu.hugegraph.structure.HugeVertex;
import com.baidu.hugegraph.utils.Constants;
import com.baidu.hugegraph.utils.HugeGraphUtils;
import com.baidu.hugegraph.utils.ValueUtils;

/**
 * Created by zhangsuochao on 17/2/7.
 */
public class VertexService extends BaseService {
    private static final Logger logger = LoggerFactory.getLogger(VertexService.class);

    public VertexService(HugeGraph graph) {
        super(graph, Constants.VERTICES);
    }

    public void addVertex(HugeVertex vertex) {
        Put put = constructInsertion(vertex);
        try {
            this.table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
            logger.error(e.getMessage());
        }

    }

    public Vertex findVertex(Object id) {
        Get get = new Get(ValueUtils.serializeWithSalt(id));
        Vertex vertex = null;
        try {
            Result result = table.get(get);
            vertex = (HugeVertex) HugeGraphUtils.parseResult(HugeElement.ElementType.VERTEX, result, this.graph);
            if (vertex == null) {
                logger.info("Vertex does not exist:{}", id);
            }
        } catch (IOException e) {
            e.printStackTrace();
            logger.error(e.getMessage());
        }
        return vertex;
    }

    /**
     * Return all vertices
     *
     * @return
     */
    public Iterator<Vertex> vertices() {
        ResultScanner scanner;
        try {
            scanner = table.getScanner(new Scan());
            return HugeGraphUtils.parseVertexScanner(scanner, this
                    .graph);
        } catch (IOException e) {
            e.printStackTrace();
            logger.error(e.getMessage());
        }
        return null;
    }

    public Iterator<Vertex> vertices(Object fromId, int limit) {
        return null;
    }

    public Iterator<Vertex> vertices(String label) {
        ResultScanner scanner;
        try {
            scanner = table.getScanner(this.getPropertyScan(label));
            return HugeGraphUtils.parseVertexScanner(scanner, this.graph);
        } catch (IOException e) {
            e.printStackTrace();
            logger.error(e.getMessage());
        }
        return null;
    }

    private Put constructInsertion(HugeVertex vertex) {
        final String label = vertex.label() != null ? vertex.label() : Vertex.DEFAULT_LABEL;
        Put put = new Put(ValueUtils.serializeWithSalt(vertex.id()));
        put.addColumn(Constants.DEFAULT_FAMILY_BYTES, Constants.LABEL_BYTES,
                ValueUtils.serialize(label));
        put.addColumn(Constants.DEFAULT_FAMILY_BYTES, Constants.CREATED_AT_BYTES,
                ValueUtils.serialize(vertex.getCreatedAt()));
        put.addColumn(Constants.DEFAULT_FAMILY_BYTES, Constants.UPDATED_AT_BYTES,
                ValueUtils.serialize(vertex.getUpdatedAt()));
        for (String key : vertex.getProperties().keySet()) {
            byte[] keyBytes = Bytes.toBytes(key);
            byte[] valueBytes = ValueUtils.serialize(vertex.getProperties().get(key));
            put.addColumn(Constants.DEFAULT_FAMILY_BYTES, keyBytes, valueBytes);
        }

        return put;
    }

    /**
     * md5(label_vertexid)_label_vertexid
     *
     * @param vertex
     *
     * @return
     */
    private String createRowkey(HugeVertex vertex) {
        String labelVertexid = vertex.label() + "_" + vertex.id().toString();
        String hash = DigestUtils.md5Hex(labelVertexid);
        return hash + "_" + labelVertexid;
    }

}
