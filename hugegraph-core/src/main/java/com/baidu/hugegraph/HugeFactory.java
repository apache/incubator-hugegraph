package com.baidu.hugegraph;

import java.util.Map;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;

/**
 * Created by jishilei on 17/3/16.
 */
public class HugeFactory {

    public static HugeGraph open(Configuration configuration) {
        return (HugeGraph) GraphFactory.open(configuration);
    }

    public static HugeGraph open(String configurationFile) {
        return (HugeGraph) GraphFactory.open(configurationFile);
    }

    public static HugeGraph open(Map configuration) {
        return (HugeGraph) GraphFactory.open(configuration);
    }

}
