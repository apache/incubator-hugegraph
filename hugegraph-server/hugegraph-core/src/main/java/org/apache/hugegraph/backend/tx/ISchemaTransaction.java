package org.apache.hugegraph.backend.tx;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.schema.EdgeLabel;
import org.apache.hugegraph.schema.IndexLabel;
import org.apache.hugegraph.schema.PropertyKey;
import org.apache.hugegraph.schema.SchemaElement;
import org.apache.hugegraph.schema.SchemaLabel;
import org.apache.hugegraph.schema.VertexLabel;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.type.define.GraphMode;
import org.apache.hugegraph.type.define.SchemaStatus;

public interface ISchemaTransaction {
    List<PropertyKey> getPropertyKeys();

    Id removePropertyKey(Id pkey);

    PropertyKey getPropertyKey(Id id);

    PropertyKey getPropertyKey(String name);

    Id clearOlapPk(PropertyKey propertyKey);

    void addVertexLabel(VertexLabel label);

    void updateVertexLabel(VertexLabel label);

    Id removeVertexLabel(Id label);

    List<VertexLabel> getVertexLabels();

    VertexLabel getVertexLabel(Id id);

    VertexLabel getVertexLabel(String name);

    List<EdgeLabel> getEdgeLabels();

    Id addPropertyKey(PropertyKey pkey);

    void updatePropertyKey(PropertyKey pkey);

    void updateEdgeLabel(EdgeLabel label);

    void addEdgeLabel(EdgeLabel label);

    Id removeEdgeLabel(Id id);

    EdgeLabel getEdgeLabel(Id id);

    EdgeLabel getEdgeLabel(String name);

    void addIndexLabel(SchemaLabel schemaLabel, IndexLabel indexLabel);

    void updateIndexLabel(IndexLabel label);

    Id removeIndexLabel(Id id);

    Id rebuildIndex(SchemaElement schema);

    Id rebuildIndex(SchemaElement schema, Set<Id> dependencies);

    List<IndexLabel> getIndexLabels();

    IndexLabel getIndexLabel(Id id);

    IndexLabel getIndexLabel(String name);

    void close();

    Id getNextId(HugeType type);

    Id validOrGenerateId(HugeType type, Id id, String name);

    void checkSchemaName(String name);

    String graphName();

    void updateSchemaStatus(SchemaElement element, SchemaStatus status);

    GraphMode graphMode();

    boolean existsSchemaId(HugeType type, Id id);

    void removeIndexLabelFromBaseLabel(IndexLabel indexLabel);

    void createIndexLabelForOlapPk(PropertyKey propertyKey);
}
