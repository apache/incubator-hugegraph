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

package com.baidu.hugegraph.backend.serializer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.id.EdgeId;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.backend.id.IdUtil;
import com.baidu.hugegraph.backend.query.Condition;
import com.baidu.hugegraph.backend.query.ConditionQuery;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.schema.EdgeLabel;
import com.baidu.hugegraph.schema.IndexLabel;
import com.baidu.hugegraph.schema.PropertyKey;
import com.baidu.hugegraph.schema.SchemaElement;
import com.baidu.hugegraph.schema.SchemaLabel;
import com.baidu.hugegraph.schema.VertexLabel;
import com.baidu.hugegraph.structure.HugeEdge;
import com.baidu.hugegraph.structure.HugeEdgeProperty;
import com.baidu.hugegraph.structure.HugeElement;
import com.baidu.hugegraph.structure.HugeIndex;
import com.baidu.hugegraph.structure.HugeProperty;
import com.baidu.hugegraph.structure.HugeVertex;
import com.baidu.hugegraph.structure.HugeVertexProperty;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.AggregateType;
import com.baidu.hugegraph.type.define.Cardinality;
import com.baidu.hugegraph.type.define.DataType;
import com.baidu.hugegraph.type.define.Directions;
import com.baidu.hugegraph.type.define.Frequency;
import com.baidu.hugegraph.type.define.HugeKeys;
import com.baidu.hugegraph.type.define.IdStrategy;
import com.baidu.hugegraph.type.define.IndexType;
import com.baidu.hugegraph.type.define.SchemaStatus;
import com.baidu.hugegraph.type.define.SerialEnum;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.JsonUtil;

public abstract class TableSerializer extends AbstractSerializer {

    @Override
    public TableBackendEntry newBackendEntry(HugeType type, Id id) {
        return new TableBackendEntry(type, id);
    }

    protected TableBackendEntry newBackendEntry(HugeElement e) {
        return newBackendEntry(e.type(), e.id());
    }

    protected TableBackendEntry newBackendEntry(SchemaElement e) {
        return newBackendEntry(e.type(), e.id());
    }

    protected TableBackendEntry newBackendEntry(HugeIndex index) {
        return newBackendEntry(index.type(), index.id());
    }

    protected abstract TableBackendEntry newBackendEntry(TableBackendEntry.Row row);

    @Override
    protected abstract TableBackendEntry convertEntry(BackendEntry backendEntry);

    protected void formatProperty(HugeProperty<?> property,
                                  TableBackendEntry.Row row) {
        long pkid = property.propertyKey().id().asLong();
        row.column(HugeKeys.PROPERTIES, pkid, this.writeProperty(property));
    }

    protected void parseProperty(Id key, Object colValue, HugeElement owner) {
        // Get PropertyKey by PropertyKey id
        PropertyKey pkey = owner.graph().propertyKey(key);

        // Parse value
        Object value = this.readProperty(pkey, colValue);

        // Set properties of vertex/edge
        if (pkey.cardinality() == Cardinality.SINGLE) {
            owner.addProperty(pkey, value);
        } else {
            if (!(value instanceof Collection)) {
                throw new BackendException(
                          "Invalid value of non-single property: %s", value);
            }
            owner.addProperty(pkey, value);
        }
    }

    protected Object writeProperty(HugeProperty<?> property) {
        return this.writeProperty(property.propertyKey(), property.value());
    }

    protected Object writeProperty(PropertyKey propertyKey, Object value) {
        return JsonUtil.toJson(value);
    }

    @SuppressWarnings("unchecked")
    protected <T> T readProperty(PropertyKey pkey, Object value) {
        Class<T> clazz = (Class<T>) pkey.implementClazz();
        T result = JsonUtil.fromJson(value.toString(), clazz);
        if (pkey.cardinality() != Cardinality.SINGLE) {
            Collection<?> values = (Collection<?>) result;
            List<Object> newValues = new ArrayList<>(values.size());
            for (Object v : values) {
                newValues.add(JsonUtil.castNumber(v, pkey.dataType().clazz()));
            }
            result = (T) newValues;
        }
        return result;
    }

    protected TableBackendEntry.Row formatEdge(HugeEdge edge) {
        EdgeId id = edge.idWithDirection();
        TableBackendEntry.Row row = new TableBackendEntry.Row(edge.type(), id);
        if (edge.hasTtl()) {
            row.ttl(edge.ttl());
        }
        // Id: ownerVertex + direction + edge-label + sortValues + otherVertex
        row.column(HugeKeys.OWNER_VERTEX, this.writeId(id.ownerVertexId()));
        row.column(HugeKeys.DIRECTION, id.directionCode());
        row.column(HugeKeys.LABEL, id.edgeLabelId().asLong());
        row.column(HugeKeys.SORT_VALUES, id.sortValues());
        row.column(HugeKeys.OTHER_VERTEX, this.writeId(id.otherVertexId()));
        row.column(HugeKeys.EXPIRED_TIME, edge.expiredTime());

        this.formatProperties(edge, row);
        return row;
    }

    /**
     * Parse an edge from a entry row
     * @param row edge entry
     * @param vertex null or the source vertex
     * @param graph the HugeGraph context object
     * @return the source vertex
     */
    protected HugeEdge parseEdge(TableBackendEntry.Row row,
                                 HugeVertex vertex, HugeGraph graph) {
        Object ownerVertexId = row.column(HugeKeys.OWNER_VERTEX);
        Number dir = row.column(HugeKeys.DIRECTION);
        Directions direction = EdgeId.directionFromCode(dir.byteValue());
        Number label = row.column(HugeKeys.LABEL);
        String sortValues = row.column(HugeKeys.SORT_VALUES);
        Object otherVertexId = row.column(HugeKeys.OTHER_VERTEX);
        Number expiredTime = row.column(HugeKeys.EXPIRED_TIME);

        if (vertex == null) {
            Id ownerId = this.readId(ownerVertexId);
            vertex = new HugeVertex(graph, ownerId, null);
        }

        EdgeLabel edgeLabel = graph.edgeLabelOrNone(this.toId(label));
        VertexLabel srcLabel = graph.vertexLabelOrNone(edgeLabel.sourceLabel());
        VertexLabel tgtLabel = graph.vertexLabelOrNone(edgeLabel.targetLabel());

        Id otherId = this.readId(otherVertexId);
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
        edge.name(sortValues);
        edge.vertices(isOutEdge, vertex, otherVertex);
        edge.assignId();

        if (isOutEdge) {
            vertex.addOutEdge(edge);
            otherVertex.addInEdge(edge.switchOwner());
        } else {
            vertex.addInEdge(edge);
            otherVertex.addOutEdge(edge.switchOwner());
        }

        vertex.propNotLoaded();
        otherVertex.propNotLoaded();

        // Parse edge properties
        this.parseProperties(edge, row);
        edge.expiredTime(expiredTime.longValue());
        return edge;
    }

    @Override
    public BackendEntry writeVertex(HugeVertex vertex) {
        TableBackendEntry entry = newBackendEntry(vertex);
        if (vertex.hasTtl()) {
            entry.ttl(vertex.ttl());
        }
        entry.column(HugeKeys.ID, this.writeId(vertex.id()));
        entry.column(HugeKeys.LABEL, vertex.schemaLabel().id().asLong());
        entry.column(HugeKeys.EXPIRED_TIME, vertex.expiredTime());
        // Add all properties of a Vertex
        this.formatProperties(vertex, entry.row());
        return entry;
    }

    @Override
    public BackendEntry writeVertexProperty(HugeVertexProperty<?> prop) {
        HugeVertex vertex = prop.element();
        TableBackendEntry entry = newBackendEntry(vertex);
        if (vertex.hasTtl()) {
            entry.ttl(vertex.ttl());
        }
        entry.subId(IdGenerator.of(prop.key()));
        entry.column(HugeKeys.ID, this.writeId(vertex.id()));
        entry.column(HugeKeys.LABEL, vertex.schemaLabel().id().asLong());
        entry.column(HugeKeys.EXPIRED_TIME, vertex.expiredTime());

        this.formatProperty(prop, entry.row());
        return entry;
    }

    @Override
    public HugeVertex readVertex(HugeGraph graph, BackendEntry backendEntry) {
        E.checkNotNull(graph, "serializer graph");
        if (backendEntry == null) {
            return null;
        }

        TableBackendEntry entry = this.convertEntry(backendEntry);
        assert entry.type().isVertex();

        Id id = this.readId(entry.column(HugeKeys.ID));
        Number label = entry.column(HugeKeys.LABEL);
        Number expiredTime = entry.column(HugeKeys.EXPIRED_TIME);

        VertexLabel vertexLabel = VertexLabel.NONE;
        if (label != null) {
            vertexLabel = graph.vertexLabelOrNone(this.toId(label));
        }
        HugeVertex vertex = new HugeVertex(graph, id, vertexLabel);

        // Parse all properties of a Vertex
        this.parseProperties(vertex, entry.row());
        // Parse all edges of a Vertex
        for (TableBackendEntry.Row edge : entry.subRows()) {
            this.parseEdge(edge, vertex, graph);
        }
        // The expired time is null when this is fake vertex of edge
        if (expiredTime != null) {
            vertex.expiredTime(expiredTime.longValue());
        }
        return vertex;
    }

    @Override
    public BackendEntry writeEdge(HugeEdge edge) {
        return newBackendEntry(this.formatEdge(edge));
    }

    @Override
    public BackendEntry writeEdgeProperty(HugeEdgeProperty<?> prop) {
        HugeEdge edge = prop.element();
        EdgeId id = edge.idWithDirection();
        TableBackendEntry.Row row = new TableBackendEntry.Row(edge.type(), id);
        if (edge.hasTtl()) {
            row.ttl(edge.ttl());
        }
        // Id: ownerVertex + direction + edge-label + sortValues + otherVertex
        row.column(HugeKeys.OWNER_VERTEX, this.writeId(id.ownerVertexId()));
        row.column(HugeKeys.DIRECTION, id.directionCode());
        row.column(HugeKeys.LABEL, id.edgeLabelId().asLong());
        row.column(HugeKeys.SORT_VALUES, id.sortValues());
        row.column(HugeKeys.OTHER_VERTEX, this.writeId(id.otherVertexId()));
        row.column(HugeKeys.EXPIRED_TIME, edge.expiredTime());

        // Format edge property
        this.formatProperty(prop, row);

        TableBackendEntry entry = newBackendEntry(row);
        entry.subId(IdGenerator.of(prop.key()));
        return entry;
    }

    @Override
    public HugeEdge readEdge(HugeGraph graph, BackendEntry backendEntry) {
        E.checkNotNull(graph, "serializer graph");
        if (backendEntry == null) {
            return null;
        }

        TableBackendEntry entry = this.convertEntry(backendEntry);
        return this.parseEdge(entry.row(), null, graph);
    }

    @Override
    public BackendEntry writeIndex(HugeIndex index) {
        TableBackendEntry entry = newBackendEntry(index);
        /*
         * When field-values is null and elementIds size is 0, it is
         * meaningful for deletion of index data in secondary/range index.
         */
        if (index.fieldValues() == null && index.elementIds().size() == 0) {
            entry.column(HugeKeys.INDEX_LABEL_ID, index.indexLabel().longId());
        } else {
            entry.column(HugeKeys.FIELD_VALUES, index.fieldValues());
            entry.column(HugeKeys.INDEX_LABEL_ID, index.indexLabel().longId());
            entry.column(HugeKeys.ELEMENT_IDS, this.writeId(index.elementId()));
            entry.column(HugeKeys.EXPIRED_TIME, index.expiredTime());
            entry.subId(index.elementId());
            if (index.hasTtl()) {
                entry.ttl(index.ttl());
            }
        }
        return entry;
    }

    @Override
    public HugeIndex readIndex(HugeGraph graph, ConditionQuery query,
                               BackendEntry backendEntry) {
        E.checkNotNull(graph, "serializer graph");
        if (backendEntry == null) {
            return null;
        }

        TableBackendEntry entry = this.convertEntry(backendEntry);

        Object indexValues = entry.column(HugeKeys.FIELD_VALUES);
        Number indexLabelId = entry.column(HugeKeys.INDEX_LABEL_ID);
        Set<Object> elemIds = this.parseIndexElemIds(entry);
        Number expiredTime = entry.column(HugeKeys.EXPIRED_TIME);

        IndexLabel indexLabel = graph.indexLabel(this.toId(indexLabelId));
        HugeIndex index = new HugeIndex(graph, indexLabel);
        index.fieldValues(indexValues);
        for (Object elemId : elemIds) {
            index.elementIds(this.readId(elemId), expiredTime.longValue());
        }
        return index;
    }

    @Override
    public BackendEntry writeId(HugeType type, Id id) {
        return newBackendEntry(type, id);
    }

    @Override
    protected Id writeQueryId(HugeType type, Id id) {
        if (type.isEdge()) {
            if (!(id instanceof EdgeId)) {
                id = EdgeId.parse(id.asString());
            }
        } else if (type.isGraph()) {
            id = IdGenerator.of(this.writeId(id));
        }
        return id;
    }

    @Override
    protected Query writeQueryEdgeCondition(Query query) {
        ConditionQuery result = (ConditionQuery) query;
        for (Condition.Relation r : result.relations()) {
            Object value = r.value();
            if (value instanceof Id) {
                if (r.key() == HugeKeys.OWNER_VERTEX ||
                    r.key() == HugeKeys.OTHER_VERTEX) {
                    // Serialize vertex id
                    r.serialValue(this.writeId((Id) value));
                } else {
                    // Serialize label id
                    r.serialValue(((Id) value).asObject());
                }
            } else if (value instanceof Directions) {
                r.serialValue(((Directions) value).type().code());
            }
        }
        return null;
    }

    @Override
    protected Query writeQueryCondition(Query query) {
        ConditionQuery result = (ConditionQuery) query;
        // No user-prop when serialize
        assert result.allSysprop();
        for (Condition.Relation r : result.relations()) {
            if (!(r.value().equals(r.serialValue()))) {
                continue;
            }
            if (r.relation() == Condition.RelationType.IN) {
                List<?> values = (List<?>) r.value();
                List<Object> serializedValues = new ArrayList<>(values.size());
                for (Object v : values) {
                    serializedValues.add(this.serializeValue(v));
                }
                r.serialValue(serializedValues);
            } else {
                r.serialValue(this.serializeValue(r.value()));
            }

            if (query.resultType().isGraph() &&
                r.relation() == Condition.RelationType.CONTAINS_VALUE) {
                r.serialValue(this.writeProperty(null, r.serialValue()));
            }
        }

        return query;
    }

    @Override
    public BackendEntry writeVertexLabel(VertexLabel vertexLabel) {
        TableBackendEntry entry = newBackendEntry(vertexLabel);
        entry.column(HugeKeys.ID, vertexLabel.id().asLong());
        entry.column(HugeKeys.NAME, vertexLabel.name());
        entry.column(HugeKeys.ID_STRATEGY, vertexLabel.idStrategy().code());
        entry.column(HugeKeys.PROPERTIES,
                     this.toLongSet(vertexLabel.properties()));
        entry.column(HugeKeys.PRIMARY_KEYS,
                     this.toLongList(vertexLabel.primaryKeys()));
        entry.column(HugeKeys.NULLABLE_KEYS,
                     this.toLongSet(vertexLabel.nullableKeys()));
        entry.column(HugeKeys.INDEX_LABELS,
                     this.toLongSet(vertexLabel.indexLabels()));
        this.writeEnableLabelIndex(vertexLabel, entry);
        this.writeUserdata(vertexLabel, entry);
        entry.column(HugeKeys.STATUS, vertexLabel.status().code());
        entry.column(HugeKeys.TTL, vertexLabel.ttl());
        entry.column(HugeKeys.TTL_START_TIME,
                     vertexLabel.ttlStartTime().asLong());
        return entry;
    }

    @Override
    public BackendEntry writeEdgeLabel(EdgeLabel edgeLabel) {
        TableBackendEntry entry = newBackendEntry(edgeLabel);
        entry.column(HugeKeys.ID, edgeLabel.id().asLong());
        entry.column(HugeKeys.NAME, edgeLabel.name());
        entry.column(HugeKeys.FREQUENCY, edgeLabel.frequency().code());
        entry.column(HugeKeys.SOURCE_LABEL, edgeLabel.sourceLabel().asLong());
        entry.column(HugeKeys.TARGET_LABEL, edgeLabel.targetLabel().asLong());
        entry.column(HugeKeys.PROPERTIES,
                     this.toLongSet(edgeLabel.properties()));
        entry.column(HugeKeys.SORT_KEYS,
                     this.toLongList(edgeLabel.sortKeys()));
        entry.column(HugeKeys.NULLABLE_KEYS,
                     this.toLongSet(edgeLabel.nullableKeys()));
        entry.column(HugeKeys.INDEX_LABELS,
                     this.toLongSet(edgeLabel.indexLabels()));
        this.writeEnableLabelIndex(edgeLabel, entry);
        this.writeUserdata(edgeLabel, entry);
        entry.column(HugeKeys.STATUS, edgeLabel.status().code());
        entry.column(HugeKeys.TTL, edgeLabel.ttl());
        entry.column(HugeKeys.TTL_START_TIME,
                     edgeLabel.ttlStartTime().asLong());
        return entry;
    }

    @Override
    public BackendEntry writePropertyKey(PropertyKey propertyKey) {
        TableBackendEntry entry = newBackendEntry(propertyKey);
        entry.column(HugeKeys.ID, propertyKey.id().asLong());
        entry.column(HugeKeys.NAME, propertyKey.name());
        entry.column(HugeKeys.DATA_TYPE, propertyKey.dataType().code());
        entry.column(HugeKeys.CARDINALITY, propertyKey.cardinality().code());
        entry.column(HugeKeys.AGGREGATE_TYPE,
                     propertyKey.aggregateType().code());
        entry.column(HugeKeys.PROPERTIES,
                     this.toLongSet(propertyKey.properties()));
        this.writeUserdata(propertyKey, entry);
        entry.column(HugeKeys.STATUS, propertyKey.status().code());
        return entry;
    }

    @Override
    public VertexLabel readVertexLabel(HugeGraph graph,
                                       BackendEntry backendEntry) {
        if (backendEntry == null) {
            return null;
        }

        TableBackendEntry entry = this.convertEntry(backendEntry);

        Number id = entry.column(HugeKeys.ID);
        String name = entry.column(HugeKeys.NAME);
        Number idStrategy = entry.column(HugeKeys.ID_STRATEGY);
        Object properties = entry.column(HugeKeys.PROPERTIES);
        Object primaryKeys = entry.column(HugeKeys.PRIMARY_KEYS);
        Object nullableKeys = entry.column(HugeKeys.NULLABLE_KEYS);
        Object indexLabels = entry.column(HugeKeys.INDEX_LABELS);
        Number status = entry.column(HugeKeys.STATUS);
        Number ttl = entry.column(HugeKeys.TTL);
        Number ttlStartTime = entry.column(HugeKeys.TTL_START_TIME);

        VertexLabel vertexLabel = new VertexLabel(graph, this.toId(id), name);
        vertexLabel.idStrategy(SerialEnum.fromCode(IdStrategy.class,
                                                   idStrategy.byteValue()));
        vertexLabel.properties(this.toIdArray(properties));
        vertexLabel.primaryKeys(this.toIdArray(primaryKeys));
        vertexLabel.nullableKeys(this.toIdArray(nullableKeys));
        vertexLabel.indexLabels(this.toIdArray(indexLabels));
        this.readEnableLabelIndex(vertexLabel, entry);
        this.readUserdata(vertexLabel, entry);
        vertexLabel.status(SerialEnum.fromCode(SchemaStatus.class,
                                               status.byteValue()));
        vertexLabel.ttl(ttl.longValue());
        vertexLabel.ttlStartTime(this.toId(ttlStartTime));
        return vertexLabel;
    }

    @Override
    public EdgeLabel readEdgeLabel(HugeGraph graph, BackendEntry backendEntry) {
        if (backendEntry == null) {
            return null;
        }

        TableBackendEntry entry = this.convertEntry(backendEntry);

        Number id = entry.column(HugeKeys.ID);
        String name = entry.column(HugeKeys.NAME);
        Number frequency = entry.column(HugeKeys.FREQUENCY);
        Number sourceLabel = entry.column(HugeKeys.SOURCE_LABEL);
        Number targetLabel = entry.column(HugeKeys.TARGET_LABEL);
        Object sortKeys = entry.column(HugeKeys.SORT_KEYS);
        Object nullableKeys = entry.column(HugeKeys.NULLABLE_KEYS);
        Object properties = entry.column(HugeKeys.PROPERTIES);
        Object indexLabels = entry.column(HugeKeys.INDEX_LABELS);
        Number status = entry.column(HugeKeys.STATUS);
        Number ttl = entry.column(HugeKeys.TTL);
        Number ttlStartTime = entry.column(HugeKeys.TTL_START_TIME);

        EdgeLabel edgeLabel = new EdgeLabel(graph, this.toId(id), name);
        edgeLabel.frequency(SerialEnum.fromCode(Frequency.class,
                                                frequency.byteValue()));
        edgeLabel.sourceLabel(this.toId(sourceLabel));
        edgeLabel.targetLabel(this.toId(targetLabel));
        edgeLabel.properties(this.toIdArray(properties));
        edgeLabel.sortKeys(this.toIdArray(sortKeys));
        edgeLabel.nullableKeys(this.toIdArray(nullableKeys));
        edgeLabel.indexLabels(this.toIdArray(indexLabels));
        this.readEnableLabelIndex(edgeLabel, entry);
        this.readUserdata(edgeLabel, entry);
        edgeLabel.status(SerialEnum.fromCode(SchemaStatus.class,
                                             status.byteValue()));
        edgeLabel.ttl(ttl.longValue());
        edgeLabel.ttlStartTime(this.toId(ttlStartTime));
        return edgeLabel;
    }

    @Override
    public PropertyKey readPropertyKey(HugeGraph graph,
                                       BackendEntry backendEntry) {
        if (backendEntry == null) {
            return null;
        }

        TableBackendEntry entry = this.convertEntry(backendEntry);

        Number id = entry.column(HugeKeys.ID);
        String name = entry.column(HugeKeys.NAME);
        Number dataType = entry.column(HugeKeys.DATA_TYPE);
        Number cardinality = entry.column(HugeKeys.CARDINALITY);
        Number aggregateType = entry.column(HugeKeys.AGGREGATE_TYPE);
        Object properties = entry.column(HugeKeys.PROPERTIES);
        Number status = entry.column(HugeKeys.STATUS);

        PropertyKey propertyKey = new PropertyKey(graph, this.toId(id), name);
        propertyKey.dataType(SerialEnum.fromCode(DataType.class,
                                                 dataType.byteValue()));
        propertyKey.cardinality(SerialEnum.fromCode(Cardinality.class,
                                                    cardinality.byteValue()));
        propertyKey.aggregateType(SerialEnum.fromCode(
                                  AggregateType.class,
                                  aggregateType.byteValue()));
        propertyKey.properties(this.toIdArray(properties));

        this.readUserdata(propertyKey, entry);
        propertyKey.status(SerialEnum.fromCode(SchemaStatus.class,
                                               status.byteValue()));
        return propertyKey;
    }

    @Override
    public BackendEntry writeIndexLabel(IndexLabel indexLabel) {
        TableBackendEntry entry = newBackendEntry(indexLabel);
        entry.column(HugeKeys.ID, indexLabel.id().asLong());
        entry.column(HugeKeys.NAME, indexLabel.name());
        entry.column(HugeKeys.BASE_TYPE, indexLabel.baseType().code());
        entry.column(HugeKeys.BASE_VALUE, indexLabel.baseValue().asLong());
        entry.column(HugeKeys.INDEX_TYPE, indexLabel.indexType().code());
        entry.column(HugeKeys.FIELDS,
                     this.toLongList(indexLabel.indexFields()));
        this.writeUserdata(indexLabel, entry);
        entry.column(HugeKeys.STATUS, indexLabel.status().code());
        return entry;
    }

    @Override
    public IndexLabel readIndexLabel(HugeGraph graph,
                                     BackendEntry backendEntry) {
        if (backendEntry == null) {
            return null;
        }

        TableBackendEntry entry = this.convertEntry(backendEntry);

        Number id = entry.column(HugeKeys.ID);
        String name = entry.column(HugeKeys.NAME);
        Number baseType = entry.column(HugeKeys.BASE_TYPE);
        Number baseValueId = entry.column(HugeKeys.BASE_VALUE);
        Number indexType = entry.column(HugeKeys.INDEX_TYPE);
        Object indexFields = entry.column(HugeKeys.FIELDS);
        Number status = entry.column(HugeKeys.STATUS);

        IndexLabel indexLabel = new IndexLabel(graph, this.toId(id), name);
        indexLabel.baseType(SerialEnum.fromCode(HugeType.class,
                                                baseType.byteValue()));
        indexLabel.baseValue(this.toId(baseValueId));
        indexLabel.indexType(SerialEnum.fromCode(IndexType.class,
                                                 indexType.byteValue()));
        indexLabel.indexFields(this.toIdArray(indexFields));
        this.readUserdata(indexLabel, entry);
        indexLabel.status(SerialEnum.fromCode(SchemaStatus.class,
                                              status.byteValue()));
        return indexLabel;
    }

    protected abstract Id toId(Number number);

    protected abstract Id[] toIdArray(Object object);

    protected abstract Object toLongSet(Collection<Id> ids);

    protected abstract Object toLongList(Collection<Id> ids);

    protected abstract Set<Object> parseIndexElemIds(TableBackendEntry entry);

    protected abstract void formatProperties(HugeElement element,
                                             TableBackendEntry.Row row);

    protected abstract void parseProperties(HugeElement element,
                                            TableBackendEntry.Row row);

    protected Object writeId(Id id) {
        return IdUtil.writeStoredString(id);
    }

    protected Id readId(Object id) {
        return IdUtil.readStoredString(id.toString());
    }

    protected Object serializeValue(Object value) {
        if (value instanceof Id) {
            value = ((Id) value).asObject();
        }
        return value;
    }

    protected void writeEnableLabelIndex(SchemaLabel schema,
                                         TableBackendEntry entry) {
        entry.column(HugeKeys.ENABLE_LABEL_INDEX, schema.enableLabelIndex());
    }

    protected void readEnableLabelIndex(SchemaLabel schema,
                                        TableBackendEntry entry) {
        Boolean enableLabelIndex = entry.column(HugeKeys.ENABLE_LABEL_INDEX);
        schema.enableLabelIndex(enableLabelIndex);
    }

    protected abstract void writeUserdata(SchemaElement schema,
                                          TableBackendEntry entry);

    protected abstract void readUserdata(SchemaElement schema,
                                         TableBackendEntry entry);
}
