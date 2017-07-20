package com.baidu.hugegraph.backend.cache;

import java.util.ArrayList;
import java.util.List;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.cache.CachedBackendStore.QueryId;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.store.BackendStore;
import com.baidu.hugegraph.backend.tx.GraphTransaction;
import com.baidu.hugegraph.config.CoreOptions;
import com.baidu.hugegraph.structure.HugeEdge;
import com.baidu.hugegraph.structure.HugeElement;
import com.baidu.hugegraph.structure.HugeVertex;

public class CachedGraphTransaction extends GraphTransaction {

    private final Cache verticesCache;
    private final Cache edgesCache;

    public CachedGraphTransaction(HugeGraph graph, BackendStore store,
                                  BackendStore indexStore) {
        super(graph, store, indexStore);
        this.verticesCache = this.cache("vertex");
        this.edgesCache = this.cache("edge");
    }

    private Cache cache(String prefix) {
        final String name = prefix + "-" + super.graph().name();
        final int capacity = super.graph().configuration()
                             .get(CoreOptions.GRAPH_CACHE_CAPACITY);
        return CacheManager.instance().cache(name, capacity);
    }

    @Override
    public Iterable<Vertex> queryVertices(Object... vertexIds) {
        List<Vertex> vertices = new ArrayList<>(vertexIds.length);
        for (Object i : vertexIds) {
            Id vid = HugeElement.getIdValue(T.id, i);
            Object v = this.verticesCache.getOrFetch(vid, id -> {
                return super.queryVertices(id).iterator().next();
            });
            vertices.add(((HugeVertex) v).copy());
        }
        return vertices;
    }

    @Override
    public Iterable<Vertex> queryVertices(Query query) {
        if (!query.ids().isEmpty() && query.conditions().isEmpty()) {
            return this.queryVertices(query.ids().toArray());
        } else {
            return super.queryVertices(query);
        }
    }

    @Override
    public Iterable<Edge> queryEdges(Query query) {
        if (query.empty()) {
            // Query all edges, don't cache it
            return super.queryEdges(query);
        }

        Object result = this.edgesCache.getOrFetch(new QueryId(query), id -> {
            return super.queryEdges(query);
        });
        @SuppressWarnings("unchecked")
        Iterable<Edge> edges = (Iterable<Edge>) result;
        return edges;
    }

    @Override
    public Vertex addVertex(HugeVertex vertex) {
        // Update vertex cache
        this.verticesCache.invalidate(vertex.id());

        return super.addVertex(vertex);
    }

    @Override
    public void removeVertex(HugeVertex vertex) {
        // Update vertex cache
        this.verticesCache.invalidate(vertex.id());

        // TODO: Use a more precise strategy to update the edge cache
        this.edgesCache.clear();

        super.removeVertex(vertex);
    }

    @Override
    public Edge addEdge(HugeEdge edge) {
        // TODO: Use a more precise strategy to update the edge cache
        this.edgesCache.clear();

        return super.addEdge(edge);
    }

    @Override
    public void removeEdge(HugeEdge edge) {
        // TODO: Use a more precise strategy to update the edge cache
        this.edgesCache.clear();

        super.removeEdge(edge);
    }
}
