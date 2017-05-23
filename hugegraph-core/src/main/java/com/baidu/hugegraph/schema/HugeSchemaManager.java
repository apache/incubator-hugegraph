package com.baidu.hugegraph.schema;

import java.util.LinkedList;
import java.util.List;

import com.baidu.hugegraph.backend.tx.SchemaTransaction;
import com.baidu.hugegraph.type.schema.EdgeLabel;
import com.baidu.hugegraph.type.schema.IndexLabel;
import com.baidu.hugegraph.type.schema.PropertyKey;
import com.baidu.hugegraph.type.schema.VertexLabel;
import com.google.common.base.Preconditions;

/**
 * Created by jishilei on 17/3/17.
 */
public class HugeSchemaManager implements SchemaManager {

    private final SchemaTransaction transaction;

    public HugeSchemaManager(SchemaTransaction transaction) {
        this.transaction = transaction;
    }

    @Override
    public PropertyKey makePropertyKey(String name) {
        PropertyKey propertyKey = new HugePropertyKey(name);
        propertyKey.transaction(this.transaction);
        return propertyKey;
    }

    @Override
    public VertexLabel makeVertexLabel(String name) {
        VertexLabel vertexLabel = new HugeVertexLabel(name);
        vertexLabel.transaction(this.transaction);
        return vertexLabel;
    }

    @Override
    public EdgeLabel makeEdgeLabel(String name) {
        EdgeLabel edgeLabel = new HugeEdgeLabel(name);
        edgeLabel.transaction(this.transaction);
        return edgeLabel;
    }

    @Override
    public IndexLabel makeIndex(String name) {
        IndexLabel indexLabel = new HugeIndexLabel(name);
        indexLabel.transaction(this.transaction);
        return indexLabel;
    }

    @Override
    public PropertyKey propertyKey(String name) {
        PropertyKey propertyKey = this.transaction.getPropertyKey(name);
        Preconditions.checkNotNull(propertyKey, "undefined propertyKey: " + name);
        return propertyKey;
    }

    @Override
    public VertexLabel vertexLabel(String name) {
        VertexLabel vertexLabel = this.transaction.getVertexLabel(name);
        Preconditions.checkNotNull(vertexLabel, "undefined vertexLabel: " + name);
        return vertexLabel;
    }

    @Override
    public EdgeLabel edgeLabel(String name) {
        EdgeLabel edgeLabel = this.transaction.getEdgeLabel(name);
        Preconditions.checkNotNull(edgeLabel, "undefined edgeLabel: " + name);
        return edgeLabel;
    }

    @Override
    public List<SchemaElement> desc() {
        List<SchemaElement> elements = new LinkedList<>();
        elements.addAll(this.transaction.getPropertyKeys());
        elements.addAll(this.transaction.getVertexLabels());
        elements.addAll(this.transaction.getEdgeLabels());
        return elements;
    }

}
