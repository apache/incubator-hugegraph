package com.baidu.hugegraph.example;

import com.baidu.hugegraph.HugeFactory;
import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.dist.RegisterUtil;

public class ExampleUtil {

    private static boolean registered = false;

    public static void registerPlugins() {
        if (registered) {
            return;
        }
        registered = true;

        RegisterUtil.registerCore();
        RegisterUtil.registerCassandra();

    }

    public static HugeGraph loadGraph() {
        registerPlugins();

        String confFile = ExampleUtil.class.getClassLoader().getResource(
                "hugegraph.properties").getPath();

        HugeGraph graph = HugeFactory.open(confFile);

        graph.clearBackend();
        graph.initBackend();

        return graph;
    }
}
