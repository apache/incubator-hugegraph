package com.baidu.hugegraph.core;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.io.IoCore;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.baidu.hugegraph.serializer.JsonSerializer;
import com.baidu.hugegraph.serializer.Serializer;
import com.baidu.hugegraph.server.HugeServer;

public final class GraphManager {

    private static final Logger logger =
            LoggerFactory.getLogger(HugeServer.class);

    private final Map<String, Graph> graphs;

    public GraphManager(final Map<String, String> graphConfs) {
        this.graphs = new ConcurrentHashMap<>();

        loadGraphs(graphConfs);
    }

    protected void loadGraphs(final Map<String, String> graphConfs) {
        graphConfs.entrySet().forEach(conf -> {
            try {
                final Graph newGraph = GraphFactory.open(conf.getValue());
                this.graphs.put(conf.getKey(), newGraph);
                logger.info("Graph '{}' was successfully configured via '{}'",
                            conf.getKey(), conf.getValue());
            } catch (RuntimeException e) {
                logger.error("Graph '{}': '{}' can't be instantiated",
                             conf.getKey(), conf.getValue(), e);
            }
        });
    }

    public Map<String, Graph> graphs() {
        return this.graphs;
    }

    public Graph graph(String name) {
        return this.graphs.get(name);
    }

    public Serializer serializer(Graph g) {
        // TODO: cache Serializer
        return new JsonSerializer(g.io(IoCore.graphson()).writer()
                   .wrapAdjacencyList(true).create());
    }

    public void rollbackAll() {
        this.graphs.entrySet().forEach(e -> {
            final Graph graph = e.getValue();
            if (graph.features().graph().supportsTransactions() &&
                graph.tx().isOpen()) {
                graph.tx().rollback();
            }
        });
    }

    public void rollback(final Set<String> graphSourceNamesToCloseTxOn) {
        closeTx(graphSourceNamesToCloseTxOn, Transaction.Status.ROLLBACK);
    }

    public void commitAll() {
        this.graphs.entrySet().forEach(e -> {
            final Graph graph = e.getValue();
            if (graph.features().graph().supportsTransactions() &&
                graph.tx().isOpen()) {
                graph.tx().commit();
            }
        });
    }

    public void commit(final Set<String> graphSourceNamesToCloseTxOn) {
        closeTx(graphSourceNamesToCloseTxOn, Transaction.Status.COMMIT);
    }

    private void closeTx(final Set<String> graphSourceNamesToCloseTxOn,
                         final Transaction.Status tx) {
        final Set<Graph> graphsToCloseTxOn = new HashSet<>();

        graphSourceNamesToCloseTxOn.forEach(r -> {
            if (this.graphs.containsKey(r)) {
                graphsToCloseTxOn.add(this.graphs.get(r));
            }
        });

        graphsToCloseTxOn.forEach(graph -> {
            if (graph.features().graph().supportsTransactions() &&
                graph.tx().isOpen()) {
                if (tx == Transaction.Status.COMMIT) {
                    graph.tx().commit();
                } else {
                    graph.tx().rollback();
                }
            }
        });
    }
}
