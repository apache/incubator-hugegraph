package com.baidu.hugegraph.core;

import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.baidu.hugegraph.HugeFactory;
import com.baidu.hugegraph.HugeGraph;

public class BaseCoreTest {

    protected static final Logger logger = LoggerFactory.getLogger(
            BaseCoreTest.class);

    public static String CONF_PATH = "hugegraph.properties";

    private HugeGraph graph = null;

    protected static HugeGraph open() {
        String confFile = BaseCoreTest.class.getClassLoader().getResource(
                CONF_PATH).getPath();
        return HugeFactory.open(confFile);
    }

    @Before
    public void initTest() {
        this.graph = open();

        this.graph.clearBackend();
        this.graph.initBackend();
    }

    @After
    public void clear() throws Exception {
        this.graph.close();
    }

    public HugeGraph graph() {
        return this.graph;
    }
}
