package com.baidu.hugegraph2;

import static com.baidu.hugegraph2.configuration.ConfigSpace.BACKEND;
import static com.baidu.hugegraph2.configuration.ConfigSpace.TABLE_GRAPH;
import static com.baidu.hugegraph2.configuration.ConfigSpace.TABLE_SCHEMA;

import java.util.Iterator;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.baidu.hugegraph2.backend.BackendException;
import com.baidu.hugegraph2.backend.store.BackendStore;
import com.baidu.hugegraph2.backend.store.BackendProviderFactory;
import com.baidu.hugegraph2.backend.store.BackendStoreProvider;
import com.baidu.hugegraph2.backend.tx.GraphTransaction;
import com.baidu.hugegraph2.backend.tx.SchemaTransaction;
import com.baidu.hugegraph2.configuration.HugeConfiguration;
import com.baidu.hugegraph2.schema.HugeSchemaManager;
import com.baidu.hugegraph2.schema.SchemaManager;
import com.baidu.hugegraph2.structure.HugeFeatures;

/**
 * Created by jishilei on 17/3/17.
 */
public class HugeGraph implements Graph {

    private static final Logger logger = LoggerFactory.getLogger(HugeGraph.class);

    protected HugeConfiguration configuration = null;
    protected HugeFeatures features = null;

    // store provider like Cassandra
    private BackendStoreProvider storeProvider = null;

    // default transactions
    private GraphTransaction graphTransaction = null;
    private SchemaTransaction schemaTransaction = null;

    public HugeGraph() {
        this(new HugeConfiguration());
    }

    public HugeGraph(HugeConfiguration configuration) {
        this.configuration = configuration;
        // TODO : get supportsPersistence from configuration;
        this.features = new HugeFeatures(true);

        try {
            this.initBackend();
        } catch (BackendException e) {
            logger.error("Failed to init backend: {}", e.getMessage());
        }
    }

    public void initBackend() throws BackendException {
        this.storeProvider = BackendProviderFactory.open(configuration.get(BACKEND));

        this.schemaTransaction = this.openSchemaTransaction();
        this.graphTransaction = this.openGraphTransaction();
    }

    public SchemaTransaction openSchemaTransaction() {
        try {
            BackendStore store = this.storeProvider.open(configuration.get(TABLE_SCHEMA));
            return new SchemaTransaction(store);
        } catch (BackendException e) {
            String message = "Failed to open schema transaction";
            logger.error("{}: {}", message, e.getMessage());
            throw new HugeException(message);
        }
    }

    public GraphTransaction openGraphTransaction() {
        try {
            BackendStore store = this.storeProvider.open(configuration.get(TABLE_GRAPH));
            return new GraphTransaction(this, store);
        } catch (BackendException e) {
            String message = "Failed to open graph transaction";
            logger.error("{}: {}", message, e.getMessage());
            throw new HugeException(message);
        }
    }

    /**
     * Construct a HugeGraph instance
     * @return
     */
//    public static HugeGraph open(final Configuration configuration) {
//        HugeConfiguration conf = new HugeConfiguration(configuration);
//        conf.copy(configuration);
//        return new HugeGraph(conf);
//    }

    public SchemaManager openSchemaManager() {
        return new HugeSchemaManager(schemaTransaction);
    }

    @Override
    public Vertex addVertex(Object... keyValues) {
        return this.graphTransaction.addVertex(keyValues);
    }

    @Override
    public <C extends GraphComputer> C compute(Class<C> aClass) throws IllegalArgumentException {
        return null;
    }

    @Override
    public GraphComputer compute() throws IllegalArgumentException {
        return null;
    }

    @Override
    public Iterator<Vertex> vertices(Object... objects) {
        return this.graphTransaction.vertices(objects);
    }

    @Override
    public Iterator<Edge> edges(Object... objects) {
        return this.graphTransaction.edges(objects);
    }

    @Override
    public Transaction tx() {
        return this.graphTransaction.tx();
    }

    @Override
    public void close() throws Exception {

    }

    @Override
    public Features features() {
        return this.features;
    }

    @Override
    public Variables variables() {
        return null;
    }

    @Override
    public Configuration configuration() {
        return null;
    }
}
