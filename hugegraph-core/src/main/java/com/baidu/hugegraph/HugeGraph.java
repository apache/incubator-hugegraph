package com.baidu.hugegraph;

import java.util.Iterator;
import java.util.function.Function;

import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.Io;
import org.apache.tinkerpop.gremlin.structure.util.AbstractThreadedTransaction;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.serializer.AbstractSerializer;
import com.baidu.hugegraph.backend.serializer.SerializerFactory;
import com.baidu.hugegraph.backend.store.BackendProviderFactory;
import com.baidu.hugegraph.backend.store.BackendStore;
import com.baidu.hugegraph.backend.store.BackendStoreProvider;
import com.baidu.hugegraph.backend.tx.GraphTransaction;
import com.baidu.hugegraph.backend.tx.SchemaTransaction;
import com.baidu.hugegraph.config.CoreOptions;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.io.HugeGraphIoRegistry;
import com.baidu.hugegraph.schema.HugeSchemaManager;
import com.baidu.hugegraph.schema.SchemaManager;
import com.baidu.hugegraph.structure.HugeFeatures;
import com.baidu.hugegraph.traversal.optimize.HugeGraphStepStrategy;
import com.baidu.hugegraph.traversal.optimize.HugeVertexStepStrategy;

/**
 * Created by jishilei on 17/3/17.
 */
@Graph.OptIn("com.baidu.hugegraph.base.HugeStructureBasicSuite")
public class HugeGraph implements Graph {

    private static final Logger logger = LoggerFactory.getLogger(HugeGraph.class);

    static {
        TraversalStrategies strategies = null;
        strategies = TraversalStrategies.GlobalCache.getStrategies(
                Graph.class).clone();
        strategies.addStrategies(
                HugeVertexStepStrategy.instance(),
                HugeGraphStepStrategy.instance());
        TraversalStrategies.GlobalCache.registerStrategies(
                HugeGraph.class, strategies);
    }

    private String name = null;

    private HugeConfig configuration = null;
    private HugeFeatures features = null;

    // store provider like Cassandra
    private BackendStoreProvider storeProvider = null;

    // default transactions
    private ThreadLocal<GraphTransaction> graphTransaction = null;
    private ThreadLocal<SchemaTransaction> schemaTransaction = null;

    public HugeGraph(HugeConfig configuration) {
        this.configuration = configuration;
        this.name = configuration.get(CoreOptions.STORE);

        this.features = new HugeFeatures(this, true);

        this.graphTransaction = new ThreadLocal<>();
        this.schemaTransaction = new ThreadLocal<>();

        try {
            this.initTransaction();
        } catch (BackendException e) {
            String message = "Failed to init backend store";
            logger.error("{}: {}", message, e.getMessage());
            throw new HugeException(message);
        }
    }

    private synchronized void initTransaction() throws HugeException {
        if (this.storeProvider == null) {
            this.storeProvider = BackendProviderFactory.open(
                    this.configuration.get(CoreOptions.BACKEND),
                    this.name);
        }

        SchemaTransaction schemaTx = this.openSchemaTransaction();
        GraphTransaction graphTx = this.openGraphTransaction();

        schemaTx.autoCommit(true);
        graphTx.autoCommit(true);

        this.schemaTransaction.set(schemaTx);
        this.graphTransaction.set(graphTx);
    }

    private void destroyTransaction() {
        GraphTransaction graphTx = this.graphTransaction.get();
        if (graphTx != null) {
            try {
                graphTx.close();
            } catch (Exception e) {
                logger.error("Failed to close GraphTransaction", e);
            }
        }

        SchemaTransaction schemaTx = this.schemaTransaction.get();
        if (schemaTx != null) {
            try {
                schemaTx.close();
            } catch (Exception e) {
                logger.error("Failed to close SchemaTransaction", e);
            }
        }

        this.graphTransaction.remove();
        this.schemaTransaction.remove();
    }

    public String name() {
        return this.name;
    }

    public void initBackend() {
        this.storeProvider.init();
    }

    public void clearBackend() {
        this.storeProvider.clear();
    }

    private SchemaTransaction openSchemaTransaction() throws HugeException {
        try {
            String name = this.configuration.get(CoreOptions.STORE_SCHEMA);
            BackendStore store = this.storeProvider.loadSchemaStore(name);
            return new SchemaTransaction(this, store);
        } catch (BackendException e) {
            String message = "Failed to open schema transaction";
            logger.error("{}: {}", message, e.getMessage());
            throw new HugeException(message);
        }
    }

    private GraphTransaction openGraphTransaction() throws HugeException {
        try {
            String graph = this.configuration.get(CoreOptions.STORE_GRAPH);
            BackendStore store = this.storeProvider.loadGraphStore(graph);

            String index = this.configuration.get(CoreOptions.STORE_INDEX);
            BackendStore indexStore = this.storeProvider.loadIndexStore(index);

            return new GraphTransaction(this, store, indexStore);
        } catch (BackendException e) {
            String message = "Failed to open graph transaction";
            logger.error("{}: {}", message, e.getMessage());
            throw new HugeException(message);
        }
    }

    public SchemaTransaction schemaTransaction() {
        SchemaTransaction schemaTx = this.schemaTransaction.get();
        if (schemaTx == null) {
            this.initTransaction();
            schemaTx = this.schemaTransaction.get();
        }
        return schemaTx;
    }

    public GraphTransaction graphTransaction() {
        GraphTransaction graphTx = this.graphTransaction.get();
        if (graphTx == null) {
            this.initTransaction();
            graphTx = this.graphTransaction.get();
        }
        return graphTx;
    }

    public SchemaManager schema() {
        return new HugeSchemaManager(this.schemaTransaction());
    }

    public GraphTransaction openTransaction() {
        return this.openGraphTransaction();
    }

    public AbstractSerializer serializer() {
        String name = this.configuration.get(CoreOptions.SERIALIZER);
        AbstractSerializer serializer = SerializerFactory.serializer(name, this);
        if (serializer == null) {
            throw new HugeException("Can't load serializer with name " + name);
        }
        return serializer;
    }

    @Override
    public Vertex addVertex(Object... keyValues) {
        return this.graphTransaction().addVertex(keyValues);
    }

    @Override
    public <C extends GraphComputer> C compute(Class<C> aClass)
            throws IllegalArgumentException {
        throw Graph.Exceptions.graphComputerNotSupported();
    }

    @Override
    public GraphComputer compute() throws IllegalArgumentException {
        throw Graph.Exceptions.graphComputerNotSupported();
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public <I extends Io> I io(final Io.Builder<I> builder) {
        return (I) builder.graph(this).onMapper(mapper ->
                mapper.addRegistry(HugeGraphIoRegistry.getInstance()))
                .create();
    }

    @Override
    public Iterator<Vertex> vertices(Object... objects) {
        if (objects.length == 0) {
            return this.graphTransaction().queryVertices();
        }
        return this.graphTransaction().queryVertices(objects);
    }

    public Iterator<Vertex> vertices(Query query) {
        return this.graphTransaction().queryVertices(query);
    }

    public Iterator<Vertex> adjacentVertices(Iterator<Edge> edges) {
        return this.graphTransaction().queryAdjacentVertices(edges);
    }

    @Override
    public Iterator<Edge> edges(Object... objects) {
        if (objects.length == 0) {
            return this.graphTransaction().queryEdges();
        }
        return this.graphTransaction().queryEdges(objects);
    }

    public Iterator<Edge> edges(Query query) {
        return this.graphTransaction().queryEdges(query);
    }

    @Override
    public Transaction tx() {
        return this.tx;
    }

    @Override
    public void close() throws HugeException {
        try {
            if (this.tx.isOpen()) {
                this.tx.close();
            }
        } finally {
            this.destroyTransaction();
            this.storeProvider.close();
        }
    }

    @Override
    public HugeFeatures features() {
        return this.features;
    }

    @Override
    public Variables variables() {
        throw Graph.Exceptions.variablesNotSupported();
    }

    @Override
    public HugeConfig configuration() {
        return this.configuration;
    }

    @Override
    public String toString() {
        return StringFactory.graphString(this, this.name());
    }

    private Transaction tx = new AbstractThreadedTransaction(this) {

        private ThreadLocal<GraphTransaction> backendTx = new ThreadLocal<>();

        @Override
        public void doOpen() {
            if (this.isOpen()) {
                return;
            }
            this.backendTx.set(graphTransaction());
            this.backendTx().autoCommit(false);
        }

        @Override
        public void doCommit() {
            this.verifyOpened();
            this.backendTx().commit();
        }

        @Override
        public void doRollback() {
            this.verifyOpened();
            this.backendTx().rollback();
        }

        @Override
        public <R> Workload<R> submit(Function<Graph, R> graphRFunction) {
            throw new UnsupportedOperationException(
                    "HugeGraph transaction does not support submit.");
        }

        @Override
        public <G extends Graph> G createThreadedTx() {
            throw new UnsupportedOperationException(
                    "HugeGraph does not support threaded transactions.");
        }

        @Override
        public boolean isOpen() {
            return this.backendTx() != null;
        }

        @Override
        public void doClose() {
            this.verifyOpened();

            // calling super will clear listeners
            super.doClose();

            this.backendTx().autoCommit(true);
            try {
                // would commit() if there is changes
                // TODO: maybe we should call commit() directly
                this.backendTx().afterWrite();
            } finally {
                this.backendTx.remove();
            }
        }

        private GraphTransaction backendTx() {
            return this.backendTx.get();
        }

        private void verifyOpened() {
            if (!this.isOpen()) {
                throw new HugeException("Transaction has not been opened");
            }
        }
    };
}
