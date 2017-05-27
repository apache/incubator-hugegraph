/*
 * Copyright (C) 2017 Baidu, Inc. All Rights Reserved.
 */
package com.baidu.hugegraph.base;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.AbstractGraphProvider;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import com.baidu.hugegraph.HugeFactory;
import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.config.CassandraOptions;
import com.baidu.hugegraph.config.CoreOptions;
import com.baidu.hugegraph.structure.HugeEdge;
import com.baidu.hugegraph.structure.HugeElement;
import com.baidu.hugegraph.structure.HugeFeatures;
import com.baidu.hugegraph.structure.HugeProperty;
import com.baidu.hugegraph.structure.HugeVertex;
import com.baidu.hugegraph.structure.HugeVertexProperty;

/**
 * Created by zhangsuochao on 17/5/3.
 */
public class HugeGraphProvider extends AbstractGraphProvider {

    private static final Set<Class> IMPLEMENTATIONS = new HashSet<Class>() {
        {
            add(HugeEdge.class);
            add(HugeElement.class);
            add(HugeGraph.class);
            add(HugeProperty.class);
            add(HugeVertex.class);
            add(HugeVertexProperty.class);
        }
    };

    @Override
    public Map<String, Object> getBaseConfiguration(String graphName, Class<?> aClass, String s1,
                                                    LoadGraphWith.GraphData graphData) {
        return new HashMap<String, Object>() {
            {
                put(Graph.GRAPH, HugeFactory.class.getName());
                put(CoreOptions.BACKEND.name(), "cassandra");
                put(CoreOptions.STORE.name(), graphName);
                put(CoreOptions.SERIALIZER.name(), "cassandra");
                put(CassandraOptions.CASSANDRA_HOST.name(), "localhost");
                put(CassandraOptions.CASSANDRA_PORT.name(), 9042);
            }
        };
    }

    @Override
    public Graph openTestGraph(final Configuration config) {

        HugeGraph graph = (HugeGraph) super.openTestGraph(config);

        graph.clearBackend();
        graph.initBackend();

        graph.schema().makePropertyKey("oid").asInt().create();
        graph.schema().makePropertyKey("communityIndex").asInt().create();
        graph.schema().makePropertyKey("test").create();
        graph.schema().makePropertyKey("data").create();

        graph.schema().makeVertexLabel("v").properties("oid").primaryKeys("oid").create();
        graph.schema().makeEdgeLabel("knows").create();

        graph.tx().open();

        return new TestGraph(graph);
    }

    @Override
    public void clear(Graph graph, Configuration configuration) throws Exception {
        if (graph != null) {
            ((TestGraph) graph).hugeGraph().clearBackend();
        }
    }

    @Override
    public Set<Class> getImplementations() {
        return IMPLEMENTATIONS;
    }

    static class TestGraph implements Graph {

        protected static int oid = 0;
        protected HugeGraph graph;

        public TestGraph(HugeGraph graph) {
            this.graph = graph;
        }

        public HugeGraph hugeGraph() {
            return this.graph;
        }

        @Override
        public Vertex addVertex(Object... keyValues) {
            if (keyValues.length == 0) {
                return this.graph.addVertex("oid", oid++);
            }
            return this.graph.addVertex(keyValues);
        }

        @Override
        public <C extends GraphComputer> C compute(Class<C> graphComputerClass) throws IllegalArgumentException {
            return this.graph.compute(graphComputerClass);
        }

        @Override
        public GraphComputer compute() throws IllegalArgumentException {
            return this.graph.compute();
        }

        @Override
        public Iterator<Vertex> vertices(Object... vertexIds) {
            return this.graph.vertices(vertexIds);
        }

        @Override
        public Iterator<Edge> edges(Object... edgeIds) {
            if (this.graph.tx().isOpen()) {
                this.graph.tx().commit();
            }
            return this.graph.edges(edgeIds);
        }

        @Override
        public Transaction tx() {
            return this.graph.tx();
        }

        @Override
        public void close() throws Exception {
            this.graph.close();
        }

        @Override
        public Variables variables() {
            return this.graph.variables();
        }

        @Override
        public Configuration configuration() {
            return this.graph.configuration();
        }

        @Override
        public HugeFeatures features() {
            return this.graph.features();
        }
    }
}
