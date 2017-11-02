/*
 * Copyright 2017 HugeGraph Authors
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package com.baidu.hugegraph.backend.store.cassandra;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.backend.query.Condition;
import com.baidu.hugegraph.backend.query.ConditionQuery;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.serializer.AbstractSerializer;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.schema.EdgeLabel;
import com.baidu.hugegraph.schema.IndexLabel;
import com.baidu.hugegraph.schema.PropertyKey;
import com.baidu.hugegraph.schema.SchemaElement;
import com.baidu.hugegraph.schema.VertexLabel;
import com.baidu.hugegraph.structure.HugeEdge;
import com.baidu.hugegraph.structure.HugeEdgeProperty;
import com.baidu.hugegraph.structure.HugeElement;
import com.baidu.hugegraph.structure.HugeIndex;
import com.baidu.hugegraph.structure.HugeProperty;
import com.baidu.hugegraph.structure.HugeVertex;
import com.baidu.hugegraph.structure.HugeVertexProperty;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.Cardinality;
import com.baidu.hugegraph.type.define.DataType;
import com.baidu.hugegraph.type.define.Directions;
import com.baidu.hugegraph.type.define.Frequency;
import com.baidu.hugegraph.type.define.HugeKeys;
import com.baidu.hugegraph.type.define.IdStrategy;
import com.baidu.hugegraph.type.define.IndexType;
import com.baidu.hugegraph.type.define.SerialEnum;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.JsonUtil;
import com.google.common.collect.ImmutableMap;

public class CassandraSerializer extends AbstractSerializer {

    @Override
    public CassandraBackendEntry newBackendEntry(HugeType type, Id id) {
        return new CassandraBackendEntry(type, id);
    }

    protected CassandraBackendEntry newBackendEntry(HugeElement e) {
        return newBackendEntry(e.type(), e.id());
    }

    protected CassandraBackendEntry newBackendEntry(SchemaElement e) {
        return newBackendEntry(e.type(), e.id());
    }

    protected CassandraBackendEntry newBackendEntry(HugeIndex index) {
        return newBackendEntry(index.type(), index.id());
    }

    @Override
    protected CassandraBackendEntry convertEntry(BackendEntry backendEntry) {
        if (!(backendEntry instanceof CassandraBackendEntry)) {
            throw new BackendException("Not supported by CassandraSerializer");
        }
        return (CassandraBackendEntry) backendEntry;
    }

    protected void parseProperty(Id key, String colValue, HugeElement owner) {
        // Get PropertyKey by PropertyKey name
        PropertyKey pkey = owner.graph().propertyKey(key);

        // Parse value
        Object value = JsonUtil.fromJson(colValue, pkey.clazz());

        // Set properties of vertex/edge
        if (pkey.cardinality() == Cardinality.SINGLE) {
            owner.addProperty(pkey, value);
        } else {
            if (!(value instanceof Collection)) {
                throw new BackendException(String.format(
                          "Invalid value of non-single property: %s",
                          colValue));
            }
            for (Object v : (Collection<?>) value) {
                v = JsonUtil.castNumber(v, pkey.dataType().clazz());
                owner.addProperty(pkey, v);
            }
        }
    }

    protected CassandraBackendEntry.Row formatEdge(HugeEdge edge) {
        CassandraBackendEntry.Row row = new CassandraBackendEntry.Row(
                                        HugeType.EDGE, edge.idWithDirection());
        // sourceVertex + direction + edge-label + sortValues + targetVertex
        row.column(HugeKeys.OWNER_VERTEX, edge.ownerVertex().id().asString());
        row.column(HugeKeys.DIRECTION, edge.direction().code());
        row.column(HugeKeys.LABEL, edge.schemaLabel().id().asLong());
        row.column(HugeKeys.SORT_VALUES, edge.name());
        row.column(HugeKeys.OTHER_VERTEX, edge.otherVertex().id().asString());

        if (!edge.hasProperties() && !edge.removed()) {
            row.column(HugeKeys.PROPERTIES, ImmutableMap.of());
        } else {
            // Format edge properties
            for (HugeProperty<?> prop : edge.getProperties().values()) {
                row.column(HugeKeys.PROPERTIES,
                           prop.propertyKey().id().asLong(),
                           JsonUtil.toJson(prop.value()));
            }
        }

        return row;
    }

    /**
     * Parse an edge from a entry row
     * @param row edge entry
     * @param vertex null or the source vertex
     * @return the source vertex
     */
    protected HugeEdge parseEdge(CassandraBackendEntry.Row row,
                                 HugeVertex vertex, HugeGraph graph) {
        String ownerVertexId = row.column(HugeKeys.OWNER_VERTEX);
        Byte dir = row.column(HugeKeys.DIRECTION);
        Directions direction = SerialEnum.fromCode(Directions.class, dir);
        Number label = row.column(HugeKeys.LABEL);
        String sortValues = row.column(HugeKeys.SORT_VALUES);
        String otherVertexId = row.column(HugeKeys.OTHER_VERTEX);

        if (vertex == null) {
            Id ownerId = IdGenerator.of(ownerVertexId);
            vertex = new HugeVertex(graph, ownerId, null);
        }

        EdgeLabel edgeLabel = graph.edgeLabel(toId(label));
        VertexLabel srcLabel = graph.vertexLabel(edgeLabel.sourceLabel());
        VertexLabel tgtLabel = graph.vertexLabel(edgeLabel.targetLabel());

        Id otherId = IdGenerator.of(otherVertexId);
        boolean isOutEdge = direction == Directions.OUT;
        HugeVertex otherVertex;
        if (isOutEdge) {
            vertex.vertexLabel(srcLabel);
            otherVertex = new HugeVertex(graph, otherId, tgtLabel);
        } else {
            vertex.vertexLabel(tgtLabel);
            otherVertex = new HugeVertex(graph, otherId, srcLabel);
        }

        HugeEdge edge = new HugeEdge(graph, null, edgeLabel);

        if (isOutEdge) {
            edge.sourceVertex(vertex);
            edge.targetVertex(otherVertex);
            vertex.addOutEdge(edge);
            otherVertex.addInEdge(edge.switchOwner());
        } else {
            edge.sourceVertex(otherVertex);
            edge.targetVertex(vertex);
            vertex.addInEdge(edge);
            otherVertex.addOutEdge(edge.switchOwner());
        }

        vertex.propNotLoaded();
        otherVertex.propNotLoaded();

        // Parse edge properties
        Map<Number, String> props = row.column(HugeKeys.PROPERTIES);
        for (Map.Entry<Number, String> prop : props.entrySet()) {
            Id pkeyId = toId(prop.getKey());
            this.parseProperty(pkeyId, prop.getValue(), edge);
        }

        edge.name(sortValues);
        edge.assignId();

        return edge;
    }

    @Override
    public BackendEntry writeVertex(HugeVertex vertex) {
        CassandraBackendEntry entry = newBackendEntry(vertex);

        entry.column(HugeKeys.ID, vertex.id().asString());
        entry.column(HugeKeys.LABEL, vertex.schemaLabel().id().asLong());

        // Add all properties of a Vertex
        for (HugeProperty<?> prop : vertex.getProperties().values()) {
            entry.column(HugeKeys.PROPERTIES, prop.propertyKey().id().asLong(),
                         JsonUtil.toJson(prop.value()));
        }

        return entry;
    }

    @Override
    public BackendEntry writeVertexProperty(HugeVertexProperty<?> prop) {
        HugeVertex vertex = prop.element();
        CassandraBackendEntry entry = newBackendEntry(vertex);
        entry.subId(IdGenerator.of(prop.key()));
        entry.column(HugeKeys.ID, vertex.id().asString());
        entry.column(HugeKeys.LABEL, vertex.schemaLabel().id().asLong());
        entry.column(HugeKeys.PROPERTIES, prop.propertyKey().id().asLong(),
                     JsonUtil.toJson(prop.value()));
        return entry;
    }

    @Override
    public HugeVertex readVertex(BackendEntry backendEntry, HugeGraph graph) {
        E.checkNotNull(graph, "serializer graph");
        if (backendEntry == null) {
            return null;
        }

        CassandraBackendEntry entry = this.convertEntry(backendEntry);

        Id id = IdGenerator.of(entry.<String>column(HugeKeys.ID));
        Number label = entry.column(HugeKeys.LABEL);

        VertexLabel vertexLabel = null;
        if (label != null) {
            vertexLabel = graph.vertexLabel(toId(label));
        }
        HugeVertex vertex = new HugeVertex(graph, id, vertexLabel);

        // Parse all properties of a Vertex
        Map<Number, String> props = entry.column(HugeKeys.PROPERTIES);
        for (Map.Entry<Number, String> prop : props.entrySet()) {
            Id pkeyId = toId(prop.getKey());
            this.parseProperty(pkeyId, prop.getValue(), vertex);
        }

        // Parse all edges of a Vertex
        for (CassandraBackendEntry.Row edge : entry.subRows()) {
            this.parseEdge(edge, vertex, graph);
        }
        return vertex;
    }

    @Override
    public BackendEntry writeEdge(HugeEdge edge) {
        return new CassandraBackendEntry(this.formatEdge(edge));
    }

    @Override
    public BackendEntry writeEdgeProperty(HugeEdgeProperty<?> prop) {
        HugeEdge edge = prop.element();
        CassandraBackendEntry.Row row = new CassandraBackendEntry.Row(
                                        HugeType.EDGE, edge.idWithDirection());
        // sourceVertex + direction + edge-label + sortValues + targetVertex
        row.column(HugeKeys.OWNER_VERTEX, edge.ownerVertex().id().asString());
        row.column(HugeKeys.DIRECTION, edge.direction().code());
        row.column(HugeKeys.LABEL, edge.schemaLabel().id().asLong());
        row.column(HugeKeys.SORT_VALUES, edge.name());
        row.column(HugeKeys.OTHER_VERTEX, edge.otherVertex().id().asString());
        // Format edge property
        row.column(HugeKeys.PROPERTIES, prop.propertyKey().id().asLong(),
                   JsonUtil.toJson(prop.value()));

        CassandraBackendEntry entry = new CassandraBackendEntry(row);
        entry.subId(IdGenerator.of(prop.key()));
        return entry;
    }

    @Override
    public HugeEdge readEdge(BackendEntry backendEntry, HugeGraph graph) {
        E.checkNotNull(graph, "serializer graph");
        if (backendEntry == null) {
            return null;
        }

        CassandraBackendEntry entry = this.convertEntry(backendEntry);

        return this.parseEdge(entry.row(), null, graph);
    }

    @Override
    public BackendEntry writeIndex(HugeIndex index) {
        CassandraBackendEntry entry = newBackendEntry(index);
        /*
         * When field-values is null and elementIds size is 0, it is
         * meaningful for deletion of index data in secondary/range index.
         */
        if (index.fieldValues() == null && index.elementIds().size() == 0) {
            entry.column(HugeKeys.INDEX_LABEL_ID, index.indexLabel().asLong());
        } else {
            entry.column(HugeKeys.FIELD_VALUES, index.fieldValues());
            entry.column(HugeKeys.INDEX_LABEL_ID, index.indexLabel().asLong());
            entry.column(HugeKeys.ELEMENT_IDS, index.elementId().asString());
            entry.subId(index.elementId());
        }
        return entry;
    }

    @Override
    public HugeIndex readIndex(BackendEntry backendEntry, HugeGraph graph) {
        E.checkNotNull(graph, "serializer graph");
        if (backendEntry == null) {
            return null;
        }

        CassandraBackendEntry entry = this.convertEntry(backendEntry);

        Object indexValues = entry.column(HugeKeys.FIELD_VALUES);
        Number indexLabelId = entry.column(HugeKeys.INDEX_LABEL_ID);
        Set<String> elementIds = entry.column(HugeKeys.ELEMENT_IDS);

        IndexLabel indexLabel = graph.indexLabel(toId(indexLabelId));
        HugeIndex index = new HugeIndex(indexLabel);
        index.fieldValues(indexValues);

        for (String id : elementIds) {
            index.elementIds(IdGenerator.of(id));
        }
        return index;
    }

    @Override
    public BackendEntry writeId(HugeType type, Id id) {
        // NOTE: Cassandra does not need to add type prefix for id
        return newBackendEntry(type, id);
    }

    @Override
    public Query writeQuery(Query query) {
        // NOTE: Cassandra does not need to add type prefix for id

        // Serialize query value for CONTAINS VALUE query
        if ((query.resultType() == HugeType.VERTEX ||
             query.resultType() == HugeType.EDGE) &&
            !query.conditions().isEmpty() && query instanceof ConditionQuery) {
            ConditionQuery result = (ConditionQuery) query;
            // No user-prop when serialize
            assert result.allSysprop();
            if (result.containsCondition(Condition.RelationType.CONTAINS)) {
                for (Condition.Relation r : result.relations()) {
                    // Serialize has-value
                    if (r.relation() == Condition.RelationType.CONTAINS) {
                        r.serialValue(JsonUtil.toJson(r.value()));
                    }
                }
            }
        }
        return query;
    }

    @Override
    public BackendEntry writeVertexLabel(VertexLabel vertexLabel) {
        CassandraBackendEntry entry = newBackendEntry(vertexLabel);
        entry.column(HugeKeys.ID, vertexLabel.id().asLong());
        entry.column(HugeKeys.NAME, vertexLabel.name());
        entry.column(HugeKeys.ID_STRATEGY, vertexLabel.idStrategy().code());
        entry.column(HugeKeys.PRIMARY_KEYS,
                     toLongList(vertexLabel.primaryKeys()));
        entry.column(HugeKeys.NULLABLE_KEYS,
                     toLongSet(vertexLabel.nullableKeys()));
        entry.column(HugeKeys.INDEX_LABELS,
                     toLongSet(vertexLabel.indexLabels()));
        entry.column(HugeKeys.PROPERTIES, toLongSet(vertexLabel.properties()));
        writeUserData(vertexLabel, entry);
        return entry;
    }

    @Override
    public BackendEntry writeEdgeLabel(EdgeLabel edgeLabel) {
        CassandraBackendEntry entry = newBackendEntry(edgeLabel);
        entry.column(HugeKeys.ID, edgeLabel.id().asLong());
        entry.column(HugeKeys.NAME, edgeLabel.name());
        entry.column(HugeKeys.FREQUENCY, edgeLabel.frequency().code());
        entry.column(HugeKeys.SOURCE_LABEL, edgeLabel.sourceLabel().asLong());
        entry.column(HugeKeys.TARGET_LABEL, edgeLabel.targetLabel().asLong());
        entry.column(HugeKeys.SORT_KEYS, toLongList(edgeLabel.sortKeys()));
        entry.column(HugeKeys.NULLABLE_KEYS,
                     toLongSet(edgeLabel.nullableKeys()));
        entry.column(HugeKeys.INDEX_LABELS,
                     toLongSet(edgeLabel.indexLabels()));
        entry.column(HugeKeys.PROPERTIES, toLongSet(edgeLabel.properties()));
        writeUserData(edgeLabel, entry);
        return entry;
    }

    @Override
    public BackendEntry writePropertyKey(PropertyKey propertyKey) {
        CassandraBackendEntry entry = newBackendEntry(propertyKey);
        entry.column(HugeKeys.ID, propertyKey.id().asLong());
        entry.column(HugeKeys.NAME, propertyKey.name());
        entry.column(HugeKeys.DATA_TYPE, propertyKey.dataType().code());
        entry.column(HugeKeys.CARDINALITY, propertyKey.cardinality().code());
        entry.column(HugeKeys.PROPERTIES, toLongSet(propertyKey.properties()));
        writeUserData(propertyKey, entry);
        return entry;
    }

    @Override
    public VertexLabel readVertexLabel(BackendEntry backendEntry) {
        if (backendEntry == null) {
            return null;
        }

        CassandraBackendEntry entry = this.convertEntry(backendEntry);

        Number id = entry.column(HugeKeys.ID);
        String name = entry.column(HugeKeys.NAME);
        Byte idStrategy = entry.column(HugeKeys.ID_STRATEGY);
        Set<Number> properties = entry.column(HugeKeys.PROPERTIES);
        List<Number> primaryKeys = entry.column(HugeKeys.PRIMARY_KEYS);
        Set<Number> nullableKeys = entry.column(HugeKeys.NULLABLE_KEYS);
        Set<Number> indexLabels = entry.column(HugeKeys.INDEX_LABELS);

        VertexLabel vertexLabel = new VertexLabel(toId(id), name);
        vertexLabel.idStrategy(SerialEnum.fromCode(IdStrategy.class,
                                                   idStrategy));
        vertexLabel.properties(toIdArray(properties));
        vertexLabel.primaryKeys(toIdArray(primaryKeys));
        vertexLabel.nullableKeys(toIdArray(nullableKeys));
        vertexLabel.indexLabels(toIdArray(indexLabels));
        readUserData(vertexLabel, entry);
        return vertexLabel;
    }

    @Override
    public EdgeLabel readEdgeLabel(BackendEntry backendEntry) {
        if (backendEntry == null) {
            return null;
        }

        CassandraBackendEntry entry = this.convertEntry(backendEntry);

        Number id = entry.column(HugeKeys.ID);
        String name = entry.column(HugeKeys.NAME);
        Byte frequency = entry.column(HugeKeys.FREQUENCY);
        Number sourceLabel = entry.column(HugeKeys.SOURCE_LABEL);
        Number targetLabel = entry.column(HugeKeys.TARGET_LABEL);
        List<Number> sortKeys = entry.column(HugeKeys.SORT_KEYS);
        Set<Number> nullableKeys = entry.column(HugeKeys.NULLABLE_KEYS);
        Set<Number> properties = entry.column(HugeKeys.PROPERTIES);
        Set<Number> indexLabels = entry.column(HugeKeys.INDEX_LABELS);

        EdgeLabel edgeLabel = new EdgeLabel(toId(id), name);
        edgeLabel.frequency(SerialEnum.fromCode(Frequency.class, frequency));
        edgeLabel.sourceLabel(toId(sourceLabel));
        edgeLabel.targetLabel(toId(targetLabel));
        edgeLabel.properties(toIdArray(properties));
        edgeLabel.sortKeys(toIdArray(sortKeys));
        edgeLabel.nullableKeys(toIdArray(nullableKeys));
        edgeLabel.indexLabels(toIdArray(indexLabels));
        readUserData(edgeLabel, entry);
        return edgeLabel;
    }

    @Override
    public PropertyKey readPropertyKey(BackendEntry backendEntry) {
        if (backendEntry == null) {
            return null;
        }

        CassandraBackendEntry entry = this.convertEntry(backendEntry);

        Number id = entry.column(HugeKeys.ID);
        String name = entry.column(HugeKeys.NAME);
        Byte dataType = entry.column(HugeKeys.DATA_TYPE);
        Byte cardinality = entry.column(HugeKeys.CARDINALITY);
        Set<Number> properties = entry.column(HugeKeys.PROPERTIES);

        PropertyKey propertyKey = new PropertyKey(toId(id),name);
        propertyKey.dataType(SerialEnum.fromCode(DataType.class, dataType));
        propertyKey.cardinality(SerialEnum.fromCode(Cardinality.class,
                                                    cardinality));
        propertyKey.properties(toIdArray(properties));

        readUserData(propertyKey, entry);
        return propertyKey;
    }

    @Override
    public BackendEntry writeIndexLabel(IndexLabel indexLabel) {
        CassandraBackendEntry entry = newBackendEntry(indexLabel);
        entry.column(HugeKeys.ID, indexLabel.id().asLong());
        entry.column(HugeKeys.NAME, indexLabel.name());
        entry.column(HugeKeys.BASE_TYPE, indexLabel.baseType().code());
        entry.column(HugeKeys.BASE_VALUE, indexLabel.baseValue().asLong());
        entry.column(HugeKeys.INDEX_TYPE, indexLabel.indexType().code());
        entry.column(HugeKeys.FIELDS, toLongList(indexLabel.indexFields()));
        return entry;
    }

    @Override
    public IndexLabel readIndexLabel(BackendEntry backendEntry) {

        if (backendEntry == null) {
            return null;
        }

        CassandraBackendEntry entry = this.convertEntry(backendEntry);

        Number id = entry.column(HugeKeys.ID);
        String name = entry.column(HugeKeys.NAME);
        Byte baseType = entry.column(HugeKeys.BASE_TYPE);
        Number baseValueId = entry.column(HugeKeys.BASE_VALUE);
        Byte indexType = entry.column(HugeKeys.INDEX_TYPE);
        List<Number> indexFields = entry.column(HugeKeys.FIELDS);

        IndexLabel indexLabel = new IndexLabel(toId(id), name);
        indexLabel.baseType(SerialEnum.fromCode(HugeType.class, baseType));
        indexLabel.baseValue(toId(baseValueId));
        indexLabel.indexType(SerialEnum.fromCode(IndexType.class, indexType));
        indexLabel.indexFields(toIdArray(indexFields));
        return indexLabel;
    }

    private static Id toId(Number number) {
        return IdGenerator.of(number.longValue());
    }

    private static Id[] toIdArray(Collection<Number> numbers) {
        Id[] ids = new Id[numbers.size()];
        int i = 0;
        for (Number number : numbers) {
            ids[i++] = toId(number);
        }
        return ids;
    }

    private static Set<Long> toLongSet(Collection<Id> ids) {
        return ids.stream().map(Id::asLong).collect(Collectors.toSet());
    }

    private static List<Long> toLongList(Collection<Id> ids) {
        return ids.stream().map(Id::asLong).collect(Collectors.toList());
    }

    private static void writeUserData(SchemaElement schema,
                                      CassandraBackendEntry entry) {
        for (Map.Entry<String, Object> e : schema.userData().entrySet()) {
            entry.column(HugeKeys.USER_DATA, e.getKey(),
                         JsonUtil.toJson(e.getValue()));
        }
    }

    private static void readUserData(SchemaElement schema,
                                     CassandraBackendEntry entry) {
        // Parse all user data of a schema element
        Map<String, String> userData = entry.column(HugeKeys.USER_DATA);
        for (String key : userData.keySet()) {
            Object value = JsonUtil.fromJson(userData.get(key), Object.class);
            schema.userData(key, value);
        }
    }
}
