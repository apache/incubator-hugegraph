package com.baidu.hugegraph2;

import org.apache.commons.configuration.Configuration;

import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;

import java.util.Map;

import com.baidu.hugegraph2.configuration.HugeConfiguration;

/**
 * Created by jishilei on 17/3/16.
 */
public class HugeFactory {

    public HugeFactory() {

    }

    public static HugeGraph open(HugeConfiguration configuration) {
//        return (HugeGraph) GraphFactory.open(configuration);
        return new HugeGraph(configuration);
    }

    public static HugeGraph open(String configurationFile) {
//        return (HugeGraph) GraphFactory.open(configurationFile);
        return new HugeGraph(new HugeConfiguration(configurationFile));
    }

//    public static HugeGraph open(Map configuration) {
//        return (HugeGraph) GraphFactory.open(configuration);
//    }

    public static HugeGraph open() {
        return new HugeGraph();
    }

}
