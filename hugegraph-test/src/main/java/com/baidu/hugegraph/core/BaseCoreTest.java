package com.baidu.hugegraph.core;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.baidu.hugegraph.HugeFactory;
import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.tx.SchemaTransaction;

public class BaseCoreTest {

    protected static final Logger logger = LoggerFactory.getLogger(
            BaseCoreTest.class);

    public static String CONF_PATH = "hugegraph.properties";

    private static HugeGraph graph = null;

    protected static HugeGraph open() {
        String confFile = BaseCoreTest.class.getClassLoader().getResource(
                CONF_PATH).getPath();
        return HugeFactory.open(confFile);
    }

    @BeforeClass
    public static void init() {
        graph = open();

        graph.clearBackend();
        graph.initBackend();
    }

    @AfterClass
    public static void clear() throws Exception {
        graph.close();
    }

    public HugeGraph graph() {
        return graph;
    }

    @Before
    public void setup() {
        clearData();
    }

    @After
    public void teardown() throws Exception {
        // pass
    }

    protected void clearData() {
        HugeGraph graph = graph();

        graph.tx().open();
        try {
            // clear vertex
            graph().traversal().V().toStream().forEach(v -> {
                v.remove();
            });

            // clear edge
            graph().traversal().E().toStream().forEach(e -> {
                e.remove();
            });

            // clear schema
            SchemaTransaction schema = graph.schemaTransaction();

            schema.getIndexLabels().stream().forEach(elem -> {
                schema.removeIndexLabel(elem.name());
            });

            schema.getEdgeLabels().stream().forEach(elem -> {
                schema.removeEdgeLabel(elem.name());
            });

            schema.getVertexLabels().stream().forEach(elem -> {
                schema.removeVertexLabel(elem.name());
            });

            schema.getPropertyKeys().stream().forEach(elem -> {
                schema.removePropertyKey(elem.name());
            });

            graph.tx().commit();
        } finally {
            graph.tx().close();
        }
    }
}
