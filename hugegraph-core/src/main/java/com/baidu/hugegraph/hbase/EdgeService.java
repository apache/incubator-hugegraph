/*
 * Copyright (C) 2017 Baidu, Inc. All Rights Reserved.
 */
package com.baidu.hugegraph.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.baidu.hugegraph.structure.HugeEdge;
import com.baidu.hugegraph.structure.HugeElement;
import com.baidu.hugegraph.structure.HugeGraph;
import com.baidu.hugegraph.utils.Constants;
import com.baidu.hugegraph.utils.HugeGraphUtils;
import com.baidu.hugegraph.utils.ValueUtils;

/**
 * Created by zhangsuochao on 17/2/7.
 */
public class EdgeService extends BaseService {

    private static final Logger logger = LoggerFactory.getLogger(EdgeService.class);

    public EdgeService(HugeGraph graph) {
        super(graph, Constants.EDGES);
    }

    public void addEdge(HugeEdge edge) {
        Put put = constructInsertion(edge);
        try {
            this.table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    /**
     * @param id
     *
     * @return
     */
    public Edge findEdge(Object id) {
        Get get = new Get(ValueUtils.serializeWithSalt(id));
        HugeEdge edge = null;
        try {
            Result result = this.table.get(get);
            edge = (HugeEdge) HugeGraphUtils.parseResult(HugeElement.ElementType.EDGE, result, graph);
            findVertexOfEdge(edge);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return edge;
    }

    /**
     * all edges
     *
     * @return
     */
    public Iterator<Edge> edges() {
        ResultScanner scanner;
        Iterator<Edge> iterator = null;
        List<Edge> edgeList = new ArrayList<>();
        try {
            scanner = table.getScanner(new Scan());
            iterator = HugeGraphUtils.parseEdgeScanner(scanner, this
                    .graph);
            HugeEdge currentEdge = null;
            while (iterator.hasNext()) {
                currentEdge = (HugeEdge) iterator.next();
                findVertexOfEdge(currentEdge);
                edgeList.add(currentEdge);
            }
        } catch (IOException e) {
            e.printStackTrace();
            logger.error(e.getMessage());
        }
        return edgeList.iterator();
    }

    /**
     * @param vertex
     * @param direction
     * @param labels
     *
     * @return
     */
    public Iterator<Edge> findVertexEdges(Vertex vertex, Direction direction, String... labels) {
        logger.info("Query edges of {} id {}", this.table.getName().getNameWithNamespaceInclAsString(), vertex.id());
        ResultScanner scanner;
        Iterator<Edge> edges = null;
        try {
            scanner = table.getScanner(constructScan(vertex, direction, labels));
            edges = HugeGraphUtils.parseEdgeScanner(scanner, this
                    .graph);
            while (edges.hasNext()) {
                findVertexOfEdge((HugeEdge) edges.next());
            }
        } catch (IOException e) {
            e.printStackTrace();
            logger.error(e.getMessage());
        }
        return edges;
    }

    /**
     * @param vertex
     * @param direction the direction of current vertex
     * @param labels
     *
     * @return
     */
    private Scan constructScan(Vertex vertex, Direction direction, String... labels) {
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        SingleColumnValueFilter vertexFilter = null;
        if (Direction.IN.equals(direction)) {
            vertexFilter = new SingleColumnValueFilter(Constants.DEFAULT_FAMILY_BYTES,
                    Constants.TO_BYTES, CompareFilter.CompareOp.EQUAL, ValueUtils.serialize(vertex.id()));
        } else if (Direction.OUT.equals(direction)) {
            vertexFilter = new SingleColumnValueFilter(Constants.DEFAULT_FAMILY_BYTES,
                    Constants.FROM_BYTES, CompareFilter.CompareOp.EQUAL, ValueUtils.serialize(vertex.id()));
        }
//        else if (Direction.BOTH.equals(direction)) {
//            // TODO
//        }

        filterList.addFilter(vertexFilter);

        SingleColumnValueFilter labelsFilter = null;
        if (labels.length > 0) {
            // TODO
            labelsFilter = new SingleColumnValueFilter(Constants.DEFAULT_FAMILY_BYTES, Constants.LABEL_BYTES,
                    CompareFilter.CompareOp.EQUAL, ValueUtils.serialize(labels[0]));
            filterList.addFilter(labelsFilter);
        }

        Scan scan = new Scan();
        scan.setFilter(filterList);
        return scan;
    }

    private Put constructInsertion(HugeEdge edge) {
        final String label = edge.label() != null ? edge.label() : Edge.DEFAULT_LABEL;
        Put put = new Put(ValueUtils.serializeWithSalt(edge.id()));
        put.addColumn(Constants.DEFAULT_FAMILY_BYTES, Constants.TO_BYTES,
                ValueUtils.serialize(edge.inVertex().id()));
        put.addColumn(Constants.DEFAULT_FAMILY_BYTES, Constants.FROM_BYTES,
                ValueUtils.serialize(edge.outVertex().id()));
        put.addColumn(Constants.DEFAULT_FAMILY_BYTES, Constants.LABEL_BYTES,
                ValueUtils.serialize(label));
        put.addColumn(Constants.DEFAULT_FAMILY_BYTES, Constants.CREATED_AT_BYTES,
                ValueUtils.serialize((edge.getCreatedAt())));
        put.addColumn(Constants.DEFAULT_FAMILY_BYTES, Constants.UPDATED_AT_BYTES,
                ValueUtils.serialize((edge.getUpdatedAt())));
        for (String key : edge.getProperties().keySet()) {
            byte[] keyBytes = Bytes.toBytes(key);
            byte[] valueBytes = ValueUtils.serialize(edge.getProperties().get(key));
            put.addColumn(Constants.DEFAULT_FAMILY_BYTES, keyBytes, valueBytes);
        }

        return put;
    }

    private void findVertexOfEdge(HugeEdge edge) {
        if (edge == null) {
            return;
        }

        // Query vertex from hbase
        Vertex inVertex = this.graph.getVertexService().findVertex(edge.getVertex(Direction.IN).id());
        edge.setInVertex(inVertex);
        Vertex outVertex = this.graph.getVertexService().findVertex(edge.getVertex(Direction.OUT).id());
        edge.setOutVertex(outVertex);
    }
}
