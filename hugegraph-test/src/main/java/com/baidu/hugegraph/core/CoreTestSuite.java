package com.baidu.hugegraph.core;

import org.apache.commons.configuration.ConfigurationException;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import com.baidu.hugegraph.dist.RegisterUtil;

@RunWith(Suite.class)
@Suite.SuiteClasses({
    VertexCoreTest.class,
    EdgeCoreTest.class,
    SchemaCoreTest.class
})
public class CoreTestSuite {

    @BeforeClass
    public static void initEnv() throws ConfigurationException {
        RegisterUtil.registerBackends();
    }
}
