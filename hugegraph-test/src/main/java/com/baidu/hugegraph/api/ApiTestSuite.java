package com.baidu.hugegraph.api;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
    VertexApiTest.class,
    EdgeApiTest.class,
    GremlinApiTest.class
})
public class ApiTestSuite {
}
