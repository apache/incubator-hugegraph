package com.baidu.hugegraph.store.meta;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.baidu.hugegraph.pd.grpc.Metapb;
import com.baidu.hugegraph.store.meta.base.GlobalMetaStore;
import com.baidu.hugegraph.store.options.MetadataOptions;
import com.baidu.hugegraph.store.pd.PdProvider;

public class GraphManager extends GlobalMetaStore {
    private PdProvider pdProvider;

    private Map<String, Graph> graphs;

    public GraphManager(MetadataOptions options, PdProvider pdProvider) {
        super(options);
        this.graphs = new ConcurrentHashMap<>();
        this.pdProvider = pdProvider;
        this.pdProvider.setGraphManager(this);
    }

    /**
     * 修改图
     * 此处不加锁，要求graph是被克隆的，进制修改原始对象
     * @param graph
     * @return
     */
    public Graph updateGraph(Graph graph) {
        this.graphs.put(graph.getGraphName(), graph);
        byte[] key = MetadataKeyHelper.getGraphKey(graph.getGraphName());
        if (graph.getProtoObj() != null) {
            put(key, graph.getProtoObj().toByteArray());
        }
        return graph;
    }

    public void load() {
        byte[] key = MetadataKeyHelper.getGraphKeyPrefix();
        List<Metapb.Graph> values = scan(Metapb.Graph.parser(), key);
        values.forEach(graph -> {
            graphs.put(graph.getGraphName(), new Graph(graph));
        });
    }

    public Map<String, Graph> getGraphs() {
        return graphs;
    }

    public Graph getGraph(String graphName) {
        return graphs.get(graphName);
    }

    public Graph getCloneGraph(String graphName){
        if ( graphs.containsKey(graphName))
            return  graphs.get(graphName).clone();
        return new Graph();
    }

    public Graph removeGraph(String graphName){
        byte[] key = MetadataKeyHelper.getGraphKey(graphName);
        delete(key);
        return graphs.remove(graphName);
    }
}
