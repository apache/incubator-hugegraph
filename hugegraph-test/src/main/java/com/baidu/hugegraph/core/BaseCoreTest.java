package com.baidu.hugegraph.core;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.baidu.hugegraph.HugeFactory;
import com.baidu.hugegraph.HugeGraph;

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
        graph().clearBackend();
        graph().initBackend();
    }

    @After
    public void teardown() throws Exception {
        // pass
    }
}
