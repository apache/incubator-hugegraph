package com.baidu.hugegraph.schema;

import java.util.LinkedList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.baidu.hugegraph.backend.tx.SchemaTransaction;
import com.baidu.hugegraph.type.schema.EdgeLabel;
import com.baidu.hugegraph.type.schema.PropertyKey;
import com.baidu.hugegraph.type.schema.VertexLabel;
import com.google.common.base.Preconditions;

/**
 * Created by jishilei on 17/3/17.
 */
public class HugeSchemaManager implements SchemaManager {

    private static final Logger logger = LoggerFactory.getLogger(HugeSchemaManager.class);

    private final SchemaTransaction transaction;

    public HugeSchemaManager(SchemaTransaction transaction) {
        this.transaction = transaction;
    }

    @Override
    public PropertyKey makePropertyKey(String name) {
        return new HugePropertyKey(name, this.transaction);
    }

    @Override
    public VertexLabel makeVertexLabel(String name) {
        return new HugeVertexLabel(name, this.transaction);
    }

    @Override
    public EdgeLabel makeEdgeLabel(String name) {
        return new HugeEdgeLabel(name, this.transaction);
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
