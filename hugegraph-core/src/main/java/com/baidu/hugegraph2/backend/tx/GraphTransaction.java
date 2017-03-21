package com.baidu.hugegraph2.backend.tx;

import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;

import com.baidu.hugegraph2.backend.BackendException;
import com.baidu.hugegraph2.backend.id.Id;
import com.baidu.hugegraph2.backend.store.BackendStore;
import com.baidu.hugegraph2.schema.base.VertexLabel;
import com.baidu.hugegraph2.schema.base.maker.SchemaManager;
import com.baidu.hugegraph2.structure.HugeGraph;
import com.baidu.hugegraph2.structure.HugeProperty;
import com.baidu.hugegraph2.structure.HugeVertex;

public class GraphTransaction extends AbstractTransaction {

    private Set<HugeVertex> vertexes;
    // parent
    private final HugeGraph graph;

    public GraphTransaction(final HugeGraph graph, BackendStore store) {
        super(store);

        this.graph = graph;
        this.vertexes = new LinkedHashSet<HugeVertex>();
    }

    @Override
    public void prepareCommit() {
        for (HugeVertex v : this.vertexes) {
            // label
            this.addEntry(v.id(), T.label.name(), v.label());

            // add all properties of a Vertex
            for (HugeProperty<?> prop : v.getProperties().values()) {
                // TODO: use serializer instead, with encoded bytes
                this.addEntry(v.id(), "property:" + prop.key(), prop.value());
            }

            // add all edges of a Vertex
            for (Edge edge : v.getEdges()) {
                // TODO: this.addEntry(v.id(), "edge:" +edge.colume(), edge);
            }
        }
        this.vertexes.clear();
    }

    public Vertex addVertex(HugeVertex vertex) {
        return this.vertexes.add(vertex) ? vertex : null;
    }

    public Vertex addVertex(Object... keyValues) throws BackendException {
        ElementHelper.legalPropertyKeyValueArray(keyValues);

        Id id = HugeVertex.getIdValue(keyValues);
        Object label = HugeVertex.getLabelValue(keyValues);

        if (label == null) {
            // Preconditions.checkArgument(label != null, "Vertex label must be not null");
            throw Element.Exceptions.labelCanNotBeNull();
        }
        else if (label instanceof String) {
            SchemaManager schema = this.graph.openSchemaManager();
            label = schema.getOrCreateVertexLabel((String) label);
        }

        assert (label instanceof VertexLabel);
        HugeVertex vertex = new HugeVertex(this.graph, id, (VertexLabel) label);
        ElementHelper.attachProperties(vertex, keyValues);

        return this.addVertex(vertex);
    }

    public Iterator<Vertex> vertices(Object... vertexIds) {
        // TODO Auto-generated method stub
        return null;
    }

    public Iterator<Edge> edges(Object... edgeIds) {
        // TODO Auto-generated method stub
        return null;
    }

    public org.apache.tinkerpop.gremlin.structure.Transaction tx() {
        // TODO Auto-generated method stub
        return null;
    }
}
