/*
 * Copyright (C) 2017 Baidu, Inc. All Rights Reserved.
 */
package com.baidu.hugegraph.structure;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.baidu.hugegraph.hbase.EdgeService;
import com.baidu.hugegraph.hbase.VertexService;
import com.baidu.hugegraph.utils.HugeGraphUtils;
import com.google.common.annotations.VisibleForTesting;

/**
 * Created by zhangsuochao on 17/1/16.
 */
// @Graph.OptIn(Graph.OptIn.SUITE_STRUCTURE_STANDARD)
@Graph.OptIn("com.baidu.hugegraph.HugeStructureBasicSuite")
public final class HugeGraph implements Graph {

    private static final Logger logger = LoggerFactory.getLogger(HugeGraph.class);
    protected VertexService vertexService = null;
    protected EdgeService edgeService = null;
    protected HugeGraphConfiguration configuration = null;

    public HugeGraph(HugeGraphConfiguration configuration) {
        this.configuration = configuration;
        this.vertexService = new VertexService(this);
        this.edgeService = new EdgeService(this);
    }

    /**
     * Construct a HugeGraph instance
     *
     * @return
     */
    public static HugeGraph open(final Configuration configuration) {
        HugeGraphConfiguration conf = new HugeGraphConfiguration();
        conf.copy(configuration);
        conf.addProperty(HugeGraphConfiguration.Keys.ZOOKEEPER_QUORUM, "sh01-sjws-tjdata20.sh01.baidu.com");
        conf.addProperty(HugeGraphConfiguration.Keys.ZOOKEEPER_CLIENTPORT, "8218");
        HugeGraphUtils.createTables(conf);
        logger.info("Open HugeGraph");
        return new HugeGraph(conf);
    }

    @Override
    public Vertex addVertex(Object... keyValues) {
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        Object idValue = ElementHelper.getIdValue(keyValues).orElse(null);
        idValue = HugeGraphUtils.generateIdIfNeeded(idValue);
        final String label = ElementHelper.getLabelValue(keyValues).orElse(Vertex.DEFAULT_LABEL);
        logger.info("Adding vertex with id:{},lable:{}", idValue, label);
        HugeVertex vertex = new HugeVertex(this, idValue, label);
        Long currentTime = System.currentTimeMillis();
        vertex.setCreatedAt(currentTime);
        vertex.setUpdatedAt(currentTime);
        vertex.setProperties(keyValues);
        vertexService.addVertex(vertex);
        return vertex;
    }

    @Override
    public <C extends GraphComputer> C compute(Class<C> graphComputerClass) throws IllegalArgumentException {
        throw Graph.Exceptions.graphComputerNotSupported();
    }

    @Override
    public GraphComputer compute() throws IllegalArgumentException {
        throw Graph.Exceptions.graphComputerNotSupported();
    }

    @Override
    public Iterator<Vertex> vertices(Object... vertexIds) {
        if (vertexIds.length == 0) {
            return vertexService.vertices();
        }
        List<Vertex> vertices = new ArrayList<>();
        Vertex v;
        for (Object id : vertexIds) {
            v = vertexService.findVertex(id);
            if (v != null) {
                vertices.add(v);
            }
        }
        return vertices.iterator();
    }

    @Override
    public Iterator<Edge> edges(Object... edgeIds) {
        if (edgeIds.length == 0) {
            //all edges
            return this.edgeService.edges();
        }

        List<Edge> edges = new ArrayList<>();
        Edge edge;
        for (Object id : edgeIds) {
            edge = this.edgeService.findEdge(id);
            if (edge != null) {
                edges.add(edge);
            }
        }

        return edges.iterator();
    }

    @Override
    public Transaction tx() {
        throw Exceptions.transactionsNotSupported();
    }

    @Override
    public Variables variables() {
        return null;
    }

    @Override
    public Configuration configuration() {
        return this.configuration;
    }

    @Override
    public Features features() {
        return new HugeGraphFeatures();
    }

    public VertexService getVertexService() {
        return this.vertexService;
    }

    public EdgeService getEdgeService() {
        return this.edgeService;
    }

    @Override
    public void close() {
        close(false);
    }

    @VisibleForTesting
    public void close(boolean clear) {
        this.edgeService.close(clear);
        this.vertexService.close(clear);
    }
}
