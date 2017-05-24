package com.baidu.hugegraph.core;

import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import com.baidu.hugegraph.server.RegisterUtil;

@RunWith(Suite.class)
@Suite.SuiteClasses({
    VertexCoreTest.class,
    EdgeCoreTest.class
})
public class CoreTestSuite {

    @BeforeClass
    public static void initEnv() {
        RegisterUtil.registerCassandra();
    }
}
