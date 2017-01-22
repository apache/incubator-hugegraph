/*
 * Copyright (C) 2017 Baidu, Inc. All Rights Reserved.
 */
package com.baidu.hugegraph;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.AbstractGraphProvider;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.structure.Graph;

import com.baidu.hugegraph.structure.HugeEdge;
import com.baidu.hugegraph.structure.HugeElement;
import com.baidu.hugegraph.structure.HugeGraph;
import com.baidu.hugegraph.structure.HugeGraphConfiguration;
import com.baidu.hugegraph.structure.HugeProperty;
import com.baidu.hugegraph.structure.HugeVertex;
import com.baidu.hugegraph.structure.HugeVertexProperty;

/**
 * Created by zhangsuochao on 17/2/13.
 */
public class HugeGraphProvider extends AbstractGraphProvider {

    private static final Set<Class> IMPLEMENTATIONS = new HashSet<Class>() {
        {
            add(HugeEdge.class);
            add(HugeElement.class);
            add(HugeGraph.class);
            add(HugeProperty.class);
            add(HugeVertex.class);
            add(HugeVertexProperty.class);
        }
    };

    @Override
    public Map<String, Object> getBaseConfiguration(final String graphName, final Class<?> test,
                                                    final String testMethodName,
                                                    final LoadGraphWith.GraphData graphData) {
        return new HashMap<String, Object>() {
            {
                put(Graph.GRAPH, HugeGraph.class.getName());
                put(HugeGraphConfiguration.Keys.ZOOKEEPER_QUORUM, "sh01-sjws-tjdata20.sh01.baidu.com");
                put(HugeGraphConfiguration.Keys.ZOOKEEPER_CLIENTPORT, "8218");
                put(HugeGraphConfiguration.Keys.GRAPH_NAMESPACE, graphName);
            }
        };
    }

    @Override
    public void clear(Graph graph, Configuration configuration) throws Exception {
        if (graph != null) {
            ((HugeGraph) graph).close(true);
        }
    }

    @Override
    public Set<Class> getImplementations() {
        return IMPLEMENTATIONS;
    }
}
