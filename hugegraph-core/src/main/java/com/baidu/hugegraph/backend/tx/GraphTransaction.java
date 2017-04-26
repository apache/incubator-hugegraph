package com.baidu.hugegraph.backend.tx;

import static com.baidu.hugegraph.backend.id.SplicingIdGenerator.ID_SPLITOR;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.backend.store.BackendStore;
import com.baidu.hugegraph.schema.SchemaManager;
import com.baidu.hugegraph.structure.HugeElement;
import com.baidu.hugegraph.structure.HugeIndex;
import com.baidu.hugegraph.structure.HugeVertex;
import com.baidu.hugegraph.type.HugeTypes;
import com.baidu.hugegraph.type.schema.IndexLabel;
import com.baidu.hugegraph.type.schema.VertexLabel;
import com.baidu.hugegraph.util.CollectionUtil;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

public class GraphTransaction extends AbstractTransaction {

    private Set<HugeVertex> vertexes;

    public GraphTransaction(final HugeGraph graph, BackendStore store) {
        super(graph, store);
        this.vertexes = new LinkedHashSet<HugeVertex>();
    }

    @Override
    protected void prepareCommit() {
        // ensure all the target vertexes (of out edges) are in this.vertexes
        for (HugeVertex source : this.vertexes) {
            Iterator<Vertex> targets = source.vertices(Direction.OUT);
            while (targets.hasNext()) {
                HugeVertex target = (HugeVertex) targets.next();
                this.vertexes.add(target);
            }
        }

        // serialize and add into super.additions
        for (HugeVertex v : this.vertexes) {
            this.addEntry(this.serializer.writeVertex(v));
            this.indexUpdate(v, false);
        }

        this.vertexes.clear();
    }

    protected void indexUpdate(HugeVertex vertex, boolean removed) {
        for (String indexName : vertex.vertexLabel().indexNames()) {
            IndexLabel indexLabel = this.graph.schemaTransaction().getIndexLabel(indexName);

            List<String> propertyValues = new LinkedList<>();
            indexLabel.indexFields().forEach(field ->
                    propertyValues.add(vertex.property(field).value().toString()));

            HugeIndex index = new HugeIndex(indexLabel);
            // TODO: I feel not confortable by use static import
            index.propertyValues(StringUtils.join(propertyValues, ID_SPLITOR));
            index.elementIds(vertex.id().asString());
            this.addEntry(this.serializer.writeIndex(index));
        }
    }

    public Vertex addVertex(HugeVertex vertex) {
        this.beforeWrite();
        vertex = this.vertexes.add(vertex) ? vertex : null;
        this.afterWrite();
        return vertex;
    }

    public Vertex addVertex(Object... keyValues) {
        ElementHelper.legalPropertyKeyValueArray(keyValues);

        Id id = HugeVertex.getIdValue(keyValues);
        Object label = HugeVertex.getLabelValue(keyValues);

        // Vertex id must be null now
        if (id != null) {
            String msg = "User defined id of Vertex is not supported";
            throw new IllegalArgumentException(msg);
        }

        if (label == null) {
            // Preconditions.checkArgument(label != null, "Vertex label must be not null");
            throw Element.Exceptions.labelCanNotBeNull();
        } else if (label instanceof String) {
            SchemaManager schema = this.graph.schema();
            label = schema.vertexLabel((String) label);
        }

        // create HugeVertex
        assert (label instanceof VertexLabel);

        // check keyValues whether contain primaryKey in definition of vertexLabel.
        Set<String> primaryKeys = ((VertexLabel) label).primaryKeys();
        Preconditions.checkArgument(CollectionUtil.containsAll(
                ElementHelper.getKeys(keyValues), primaryKeys),
                "The primary key(s) must be setted: " + primaryKeys);

        HugeVertex vertex = new HugeVertex(this.graph, id, (VertexLabel) label);
        // set properties
        ElementHelper.attachProperties(vertex, keyValues);

        // generate an id and assign it if not exists
        if (id == null) {
            vertex.assignId();
        }

        return this.addVertex(vertex);
    }

    public Iterator<Vertex> queryVertices(Object... vertexIds) {
        List<Vertex> list = new ArrayList<Vertex>(vertexIds.length);

        for (Object vertexId : vertexIds) {
            Id id = HugeElement.getIdValue(T.id, vertexId);
            BackendEntry entry = super.get(HugeTypes.VERTEX, id);
            Vertex vertex = this.serializer.readVertex(entry);
            assert vertex != null;
            list.add(vertex);
        }

        return list.iterator();
    }

    public Iterator<Vertex> queryVertices(Query q) {
        List<Vertex> list = new ArrayList<Vertex>();

        Iterator<BackendEntry> entries = super.query(q).iterator();
        while (entries.hasNext()) {
            Vertex vertex = this.serializer.readVertex(entries.next());
            assert vertex != null;
            list.add(vertex);
        }

        return list.iterator();
    }

    public Iterator<Edge> queryEdges(Object... edgeIds) {
        List<Edge> list = new ArrayList<Edge>(edgeIds.length);

        for (Object edgeId : edgeIds) {
            Id id = HugeElement.getIdValue(T.id, edgeId);
            BackendEntry entry = super.get(HugeTypes.EDGE, id);
            Vertex vertex = this.serializer.readVertex(entry);
            assert vertex != null;
            list.addAll(ImmutableList.copyOf(vertex.edges(Direction.BOTH)));
        }

        return list.iterator();
    }

    public Iterator<Edge> queryEdges(Query q) {
        Iterator<Vertex> vertices = this.queryVertices(q);

        List<Edge> list = new ArrayList<Edge>();
        while (vertices.hasNext()) {
            Vertex vertex = vertices.next();
            list.addAll(ImmutableList.copyOf(vertex.edges(Direction.BOTH)));
        }

        return list.iterator();
    }
}
