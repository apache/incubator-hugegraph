package com.baidu.hugegraph2.structure;

import com.baidu.hugegraph2.configuration.HugeConfiguration;
import com.baidu.hugegraph2.schema.HugeSchemaManager;
import com.baidu.hugegraph2.schema.base.maker.SchemaManager;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by jishilei on 17/3/17.
 */
public class HugeGraph implements Graph {

    private static final Logger logger = LoggerFactory.getLogger(HugeGraph.class);
    protected HugeConfiguration configuration = null;
    protected HugeFeatures features = null;

    public HugeGraph() {
        this(new HugeConfiguration());
    }

    public HugeGraph(HugeConfiguration configuration) {
        this.configuration = configuration;
        // TODO : get supportsPersistence from configuration;
        this.features = new HugeFeatures(true);

    }

    /**
     * Construct a HugeGraph instance
     *
     * @return
     */
    public static HugeGraph open(final Configuration configuration) {
        HugeConfiguration conf = new HugeConfiguration();
        conf.copy(configuration);
        return new HugeGraph(conf);
    }

    public static SchemaManager openSchemaManager() {
        return new HugeSchemaManager();
    }

    @Override
    public Vertex addVertex(Object... objects) {
        return null;
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
        return null;
    }

    @Override
    public Iterator<Edge> edges(Object... objects) {
        return null;
    }

    @Override
    public Transaction tx() {
        return null;
    }

    @Override
    public void close() throws Exception {

    }

    @Override
    public Features features() {
        return features;
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
