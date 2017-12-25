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
import java.util.Map;

import org.apache.commons.lang.NotImplementedException;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.backend.id.SplicingIdGenerator;
import com.baidu.hugegraph.backend.query.Condition;
import com.baidu.hugegraph.backend.query.ConditionQuery;
import com.baidu.hugegraph.backend.query.IdQuery;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.exception.NotFoundException;
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
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.JsonUtil;
import com.google.common.collect.ImmutableList;

public class TextSerializer extends AbstractSerializer {

    private static final String COLUMN_SPLITOR = TextBackendEntry.COLUMN_SPLITOR;
    private static final String VALUE_SPLITOR = TextBackendEntry.VALUE_SPLITOR;

    @Override
    public TextBackendEntry newBackendEntry(HugeType type, Id id) {
        return new TextBackendEntry(type, id);
    }

    private TextBackendEntry newBackendEntry(HugeElement elem) {
        Id id = IdGenerator.of(writeId(elem.id()));
        return new TextBackendEntry(elem.type(), id);
    }

    private TextBackendEntry newBackendEntry(SchemaElement elem) {
        Id id = IdGenerator.of(writeId(elem.id()));
        return new TextBackendEntry(elem.type(), id);
    }

    @Override
    protected BackendEntry convertEntry(BackendEntry backendEntry) {
        if (!(backendEntry instanceof TextBackendEntry)) {
            throw new HugeException("The entry '%s' is not TextBackendEntry",
                                    backendEntry);
        }
        return backendEntry;
    }

    protected String formatSyspropName(String name) {
        return String.format("%s%s%s",
                             writeType(HugeType.SYS_PROPERTY),
                             COLUMN_SPLITOR, name);
    }

    protected String formatSyspropName(HugeKeys col) {
        return this.formatSyspropName(col.string());
    }

    protected String formatPropertyName(Object key) {
        return String.format("%s%s%s",
                             writeType(HugeType.PROPERTY), COLUMN_SPLITOR, key);
    }

    protected String formatPropertyName(HugeProperty<?> prop) {
        return formatPropertyName(prop.propertyKey().id());
    }

    protected String formatPropertyValue(HugeProperty<?> prop) {
        // May be a single value or a list of values
        return JsonUtil.toJson(prop.value());
    }

    protected void parseProperty(String colName, String colValue,
                                 HugeElement owner) {
        String[] colParts = colName.split(COLUMN_SPLITOR);
        assert colParts.length == 2 : colName;

        // Get PropertyKey by PropertyKey id
        PropertyKey pkey = owner.graph().propertyKey(readId(colParts[1]));

        // Parse value
        Object value = JsonUtil.fromJson(colValue, pkey.clazz());

        // Set properties of vertex/edge
        if (pkey.cardinality() == Cardinality.SINGLE) {
            owner.addProperty(pkey, value);
        } else {
            if (!(value instanceof Collection)) {
                throw new BackendException(
                          "Invalid value of non-sigle property: %s", colValue);
            }
            for (Object v : (Collection<?>) value) {
                v = JsonUtil.castNumber(v, pkey.dataType().clazz());
                owner.addProperty(pkey, v);
            }
        }
    }

    protected String formatEdgeName(HugeEdge edge) {
        // Edge name: type + edge-label-name + sortKeys + targetVertex
        StringBuilder sb = new StringBuilder(256);
        sb.append(writeType(edge.type()));
        sb.append(COLUMN_SPLITOR);
        sb.append(writeId(edge.schemaLabel().id()));
        sb.append(COLUMN_SPLITOR);
        sb.append(edge.name());
        sb.append(COLUMN_SPLITOR);
        sb.append(writeId(edge.otherVertex().id()));
        return sb.toString();
    }

    protected String formatEdgeValue(HugeEdge edge) {
        StringBuilder sb = new StringBuilder(256 * edge.getProperties().size());
        // Edge id
        sb.append(edge.id().asString());
        // Edge properties
        for (HugeProperty<?> property : edge.getProperties().values()) {
            sb.append(VALUE_SPLITOR);
            sb.append(this.formatPropertyName(property));
            sb.append(VALUE_SPLITOR);
            sb.append(this.formatPropertyValue(property));
        }
        return sb.toString();
    }

    /**
     * Parse an edge from a column item
     */
    protected void parseEdge(String colName, String colValue,
                             HugeVertex vertex) {
        String[] colParts = colName.split(COLUMN_SPLITOR);

        HugeGraph graph = vertex.graph();
        EdgeLabel label = graph.edgeLabel(readId(colParts[1]));

        VertexLabel sourceLabel = graph.vertexLabel(label.sourceLabel());
        VertexLabel targetLabel = graph.vertexLabel(label.targetLabel());

        // TODO: how to construct targetVertex with id
        Id otherVertexId = readId(colParts[3]);

        boolean isOutEdge = colParts[0].equals(writeType(HugeType.EDGE_OUT));
        HugeVertex otherVertex;
        if (isOutEdge) {
            vertex.vertexLabel(sourceLabel);
            otherVertex = new HugeVertex(graph, otherVertexId, targetLabel);
        } else {
            vertex.vertexLabel(targetLabel);
            otherVertex = new HugeVertex(graph, otherVertexId, sourceLabel);
        }

        String[] valParts = colValue.split(VALUE_SPLITOR);
        Id id = IdGenerator.of(valParts[0]);

        HugeEdge edge = new HugeEdge(graph, id, label);
        edge.name(colParts[2]);

        if (isOutEdge) {
            edge.targetVertex(otherVertex);
            vertex.addOutEdge(edge);
        } else {
            edge.sourceVertex(otherVertex);
            vertex.addInEdge(edge);
        }

        otherVertex.propNotLoaded();

        // Edge properties
        for (int i = 1; i < valParts.length; i += 2) {
            this.parseProperty(valParts[i], valParts[i + 1], edge);
        }
    }

    protected void parseColumn(String colName, String colValue,
                               HugeVertex vertex) {
        // Column name
        String type = colName.split(COLUMN_SPLITOR, 2)[0];
        // Parse property
        if (type.equals(writeType(HugeType.PROPERTY))) {
            this.parseProperty(colName, colValue, vertex);
        }
        // Parse edge
        if (type.equals(writeType(HugeType.EDGE_OUT)) ||
            type.equals(writeType(HugeType.EDGE_IN))) {
            this.parseEdge(colName, colValue, vertex);
        }
    }

    @Override
    public BackendEntry writeVertex(HugeVertex vertex) {
        TextBackendEntry entry = newBackendEntry(vertex);

        // Write label (NOTE: maybe just with edges if label is null)
        if (vertex.schemaLabel() != null) {
            entry.column(this.formatSyspropName(HugeKeys.LABEL),
                         writeId(vertex.schemaLabel().id()));
        }

        // Add all properties of a Vertex
        for (HugeProperty<?> prop : vertex.getProperties().values()) {
            entry.column(this.formatPropertyName(prop),
                         this.formatPropertyValue(prop));
        }

        return entry;
    }

    @Override
    public BackendEntry writeVertexProperty(HugeVertexProperty<?> prop) {
        HugeVertex vertex = prop.element();
        TextBackendEntry entry = newBackendEntry(vertex);
        entry.subId(IdGenerator.of(prop.key()));

        // Write label (NOTE: maybe just with edges if label is null)
        if (vertex.schemaLabel() != null) {
            entry.column(this.formatSyspropName(HugeKeys.LABEL),
                         writeId(vertex.schemaLabel().id()));
        }

        entry.column(this.formatPropertyName(prop),
                     this.formatPropertyValue(prop));
        return entry;
    }

    @Override
    public HugeVertex readVertex(BackendEntry bytesEntry, HugeGraph graph) {
        E.checkNotNull(graph, "serializer graph");
        if (bytesEntry == null) {
            return null;
        }
        bytesEntry = this.convertEntry(bytesEntry);
        assert bytesEntry instanceof TextBackendEntry;
        TextBackendEntry entry = (TextBackendEntry) bytesEntry;

        // Parse label
        String labelId = entry.column(this.formatSyspropName(HugeKeys.LABEL));
        VertexLabel label = null;
        if (labelId != null) {
            label = graph.vertexLabel(readId(labelId));
        }

        HugeVertex vertex = new HugeVertex(graph, readId(entry.id()), label);

        // Parse all properties or edges of a Vertex
        for (String name : entry.columnNames()) {
            this.parseColumn(name, entry.column(name), vertex);
        }

        return vertex;
    }

    @Override
    public BackendEntry writeEdge(HugeEdge edge) {
        Id id = writeEdgeId(edge.idWithDirection());
        TextBackendEntry entry = newBackendEntry(HugeType.EDGE, id);
        entry.column(this.formatEdgeName(edge), this.formatEdgeValue(edge));
        return entry;
    }

    @Override
    public BackendEntry writeEdgeProperty(HugeEdgeProperty<?> prop) {
        HugeEdge edge = prop.element();
        Id id = writeEdgeId(edge.idWithDirection());
        TextBackendEntry entry = newBackendEntry(HugeType.EDGE, id);
        entry.subId(IdGenerator.of(prop.key()));
        entry.column(this.formatEdgeName(edge), this.formatEdgeValue(edge));
        return entry;
    }

    @Override
    public HugeEdge readEdge(BackendEntry backendEntry, HugeGraph graph) {
        E.checkNotNull(graph, "serializer graph");
        // TODO: implement
        throw new NotImplementedException("Unsupported readEdge()");
    }

    @Override
    public BackendEntry writeIndex(HugeIndex index) {
        TextBackendEntry entry = newBackendEntry(index.type(), index.id());
        /*
         * When field-values is null and elementIds size is 0, it is
         * meaningful for deletion of index data in secondary/range index.
         */
        if (index.fieldValues() == null && index.elementIds().size() == 0) {
            entry.column(formatSyspropName(HugeKeys.INDEX_LABEL_ID),
                         writeId(index.indexLabel()));
        } else {
            // TODO: field-values may be a number (range index)
            entry.column(formatSyspropName(HugeKeys.FIELD_VALUES),
                         JsonUtil.toJson(index.fieldValues()));
            entry.column(formatSyspropName(HugeKeys.INDEX_LABEL_ID),
                         writeId(index.indexLabel()));
            entry.column(formatSyspropName(HugeKeys.ELEMENT_IDS),
                         writeIds(index.elementIds()));
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

        backendEntry = convertEntry(backendEntry);
        assert backendEntry instanceof TextBackendEntry;
        TextBackendEntry entry = (TextBackendEntry) backendEntry;

        String indexValues = entry.column(
                formatSyspropName(HugeKeys.FIELD_VALUES));
        String indexLabelId = entry.column(
                formatSyspropName(HugeKeys.INDEX_LABEL_ID));
        String elementIds = entry.column(
                formatSyspropName(HugeKeys.ELEMENT_IDS));

        IndexLabel indexLabel = IndexLabel.label(graph, readId(indexLabelId));
        HugeIndex index = new HugeIndex(indexLabel);
        index.fieldValues(JsonUtil.fromJson(indexValues, Object.class));
        index.elementIds(readIds(elementIds));
        return index;
    }

    @Override
    public TextBackendEntry writeId(HugeType type, Id id) {
        if (type == HugeType.EDGE) {
            id = writeEdgeId(id);
        } else {
            id = IdGenerator.of(writeId(id));
        }
        return newBackendEntry(type, id);
    }

    @Override
    public Query writeQuery(Query query) {
        /*
         * Serialize edge query by id/conditions to query by
         * src-vertex + edge-name.
         */
        if (query.resultType() == HugeType.EDGE && query instanceof IdQuery) {
            return this.writeEdgeQuery((IdQuery) query);
        }

        // Serialize query key/value
        if (!query.conditions().isEmpty() && query instanceof ConditionQuery) {
            ConditionQuery result = (ConditionQuery) query;
            // No user-prop when serialize
            assert result.allSysprop();
            for (Condition.Relation r : result.relations()) {
                // Serialize key
                if (SchemaElement.isSchema(query.resultType())) {
                    r.serialKey(((HugeKeys) r.key()).string());
                } else {
                    r.serialKey(formatSyspropName((HugeKeys) r.key()));
                }

                if (r.relation() == Condition.RelationType.CONTAINS_KEY) {
                    // Serialize has-key
                    r.serialValue(formatPropertyName(r.value()));
                } else if (r.value() instanceof Id) {
                    // Serialize id value
                    r.serialValue(writeId((Id) r.value()));
                } else {
                    // Serialize other type value
                    r.serialValue(JsonUtil.toJson(r.value()));
                }
            }
        }

        // Serialize id
        if (query instanceof IdQuery && !query.ids().isEmpty()) {
            IdQuery result = (IdQuery) query.copy();
            result.resetIds();
            for (Id id : query.ids()) {
                result.query(IdGenerator.of(writeId(id)));
            }
            return result;
        }
        return query;
    }

    protected IdQuery writeEdgeQuery(IdQuery query) {
        IdQuery result = query.copy();
        result.resetIds();

        if (!query.conditions().isEmpty() && !query.ids().isEmpty()) {
            throw new BackendException("Not supported query edge by id(s) " +
                                       "and condition(s) at the same time");
        }

        // By id
        if (query.ids().size() > 0) {
            for (Id id : query.ids()) {
                result.query(writeEdgeId(id));
            }
            return result;
        }

        // By condition (then convert the query to query by id)
        List<String> condParts = new ArrayList<>(query.conditions().size());

        HugeKeys[] keys = new HugeKeys[] {
                HugeKeys.OWNER_VERTEX,
                HugeKeys.DIRECTION,
                HugeKeys.LABEL,
                HugeKeys.SORT_VALUES,
                HugeKeys.OTHER_VERTEX
        };

        for (HugeKeys key : keys) {
            Object value = ((ConditionQuery) query).condition(key);
            if (value == null) {
                break;
            }
            // Serialize value
            if (value instanceof Directions) {
                value = writeType(((Directions) value).type());
            }
            if (value instanceof Id) {
                condParts.add(writeId((Id) value));
            } else {
                condParts.add(value.toString());
            }
            ((ConditionQuery) result).unsetCondition(key);
        }

        if (condParts.size() > 0) {
            // Conditions to id
            result.query(SplicingIdGenerator.concat(
                         condParts.toArray(new String[0])));
        }

        if (result.conditions().size() > 0) {
            // Like query by edge label
            assert result.ids().isEmpty();
        }

        return result;
    }

    protected static Id writeEdgeId(Id id) {
        // TODO: improve edge id split
        List<String> idParts = new ArrayList<>(ImmutableList.copyOf(
                                               SplicingIdGenerator.split(id)));
        /*
         * Note that we assume the id without Direction if it contains
         * 4 parts.
         */
        if (idParts.size() == 4) {
            // Ensure edge id with Direction
            idParts.add(1, writeType(HugeType.EDGE_OUT));
        }

        if (idParts.size() != 5) {
            throw new NotFoundException("Unsupported ID format: %s", id);
        }

        String[] sidParts = new String[idParts.size()];
        sidParts[0] = writeId(IdGenerator.of(idParts.get(0)));
        sidParts[1] = idParts.get(1);
        sidParts[2] = writeId(IdGenerator.of(Long.valueOf(idParts.get(2))));
        sidParts[3] = idParts.get(3);
        sidParts[4] = writeId(IdGenerator.of(idParts.get(4)));

        return SplicingIdGenerator.concat(sidParts);
    }

    @Override
    public BackendEntry writeVertexLabel(VertexLabel vertexLabel) {
        TextBackendEntry entry = newBackendEntry(vertexLabel);
        entry.column(HugeKeys.NAME, JsonUtil.toJson(vertexLabel.name()));
        entry.column(HugeKeys.ID_STRATEGY,
                     JsonUtil.toJson(vertexLabel.idStrategy()));
        entry.column(HugeKeys.PRIMARY_KEYS,
                     writeIds(vertexLabel.primaryKeys()));
        entry.column(HugeKeys.NULLABLE_KEYS,
                     writeIds(vertexLabel.nullableKeys()));
        entry.column(HugeKeys.INDEX_LABELS,
                     writeIds(vertexLabel.indexLabels()));
        entry.column(HugeKeys.PROPERTIES,
                     writeIds(vertexLabel.properties()));
        writeUserData(vertexLabel, entry);
        return entry;
    }

    @Override
    public VertexLabel readVertexLabel(BackendEntry backendEntry) {
        if (backendEntry == null) {
            return null;
        }

        backendEntry = convertEntry(backendEntry);
        assert backendEntry instanceof TextBackendEntry;

        TextBackendEntry entry = (TextBackendEntry) backendEntry;
        String name = JsonUtil.fromJson(entry.column(HugeKeys.NAME),
                                        String.class);
        String idStrategy = entry.column(HugeKeys.ID_STRATEGY);
        String properties = entry.column(HugeKeys.PROPERTIES);
        String primaryKeys = entry.column(HugeKeys.PRIMARY_KEYS);
        String nullableKeys = entry.column(HugeKeys.NULLABLE_KEYS);
        String indexLabels = entry.column(HugeKeys.INDEX_LABELS);

        VertexLabel vertexLabel = new VertexLabel(readId(entry.id()), name);
        vertexLabel.idStrategy(JsonUtil.fromJson(idStrategy, IdStrategy.class));
        vertexLabel.properties(readIds(properties));
        vertexLabel.primaryKeys(readIds(primaryKeys));
        vertexLabel.nullableKeys(readIds(nullableKeys));
        vertexLabel.indexLabels(readIds(indexLabels));
        readUserData(vertexLabel, entry);
        return vertexLabel;
    }

    @Override
    public BackendEntry writeEdgeLabel(EdgeLabel edgeLabel) {
        TextBackendEntry entry = newBackendEntry(edgeLabel);
        entry.column(HugeKeys.NAME, JsonUtil.toJson(edgeLabel.name()));
        entry.column(HugeKeys.SOURCE_LABEL, writeId(edgeLabel.sourceLabel()));
        entry.column(HugeKeys.TARGET_LABEL, writeId(edgeLabel.targetLabel()));
        entry.column(HugeKeys.FREQUENCY,
                     JsonUtil.toJson(edgeLabel.frequency()));
        entry.column(HugeKeys.SORT_KEYS,
                     writeIds(edgeLabel.sortKeys()));
        entry.column(HugeKeys.NULLABLE_KEYS,
                     writeIds(edgeLabel.nullableKeys()));
        entry.column(HugeKeys.INDEX_LABELS,
                     writeIds(edgeLabel.indexLabels()));
        entry.column(HugeKeys.PROPERTIES,
                     writeIds(edgeLabel.properties()));
        writeUserData(edgeLabel, entry);
        return entry;
    }

    @Override
    public EdgeLabel readEdgeLabel(BackendEntry backendEntry) {
        if (backendEntry == null) {
            return null;
        }

        backendEntry = convertEntry(backendEntry);
        assert backendEntry instanceof TextBackendEntry;

        TextBackendEntry entry = (TextBackendEntry) backendEntry;
        String name = JsonUtil.fromJson(entry.column(HugeKeys.NAME),
                                        String.class);
        String sourceLabel = entry.column(HugeKeys.SOURCE_LABEL);
        String targetLabel = entry.column(HugeKeys.TARGET_LABEL);
        String frequency = entry.column(HugeKeys.FREQUENCY);
        String sortKeys = entry.column(HugeKeys.SORT_KEYS);
        String nullablekeys = entry.column(HugeKeys.NULLABLE_KEYS);
        String properties = entry.column(HugeKeys.PROPERTIES);
        String indexLabels = entry.column(HugeKeys.INDEX_LABELS);

        EdgeLabel edgeLabel = new EdgeLabel(readId(entry.id()), name);
        edgeLabel.sourceLabel(readId(sourceLabel));
        edgeLabel.targetLabel(readId(targetLabel));
        edgeLabel.frequency(JsonUtil.fromJson(frequency, Frequency.class));
        edgeLabel.properties(readIds(properties));
        edgeLabel.sortKeys(readIds(sortKeys));
        edgeLabel.nullableKeys(readIds(nullablekeys));
        edgeLabel.indexLabels(readIds(indexLabels));
        readUserData(edgeLabel, entry);
        return edgeLabel;
    }

    @Override
    public BackendEntry writePropertyKey(PropertyKey propertyKey) {
        TextBackendEntry entry = newBackendEntry(propertyKey);
        entry.column(HugeKeys.NAME, JsonUtil.toJson(propertyKey.name()));
        entry.column(HugeKeys.DATA_TYPE,
                     JsonUtil.toJson(propertyKey.dataType()));
        entry.column(HugeKeys.CARDINALITY,
                     JsonUtil.toJson(propertyKey.cardinality()));
        entry.column(HugeKeys.PROPERTIES,
                     writeIds(propertyKey.properties()));
        writeUserData(propertyKey, entry);
        return entry;
    }

    @Override
    public PropertyKey readPropertyKey(BackendEntry backendEntry) {
        if (backendEntry == null) {
            return null;
        }

        backendEntry = convertEntry(backendEntry);
        assert backendEntry instanceof TextBackendEntry;

        TextBackendEntry entry = (TextBackendEntry) backendEntry;
        String name = JsonUtil.fromJson(entry.column(HugeKeys.NAME),
                                        String.class);
        String dataType = entry.column(HugeKeys.DATA_TYPE);
        String cardinality = entry.column(HugeKeys.CARDINALITY);
        String properties = entry.column(HugeKeys.PROPERTIES);

        PropertyKey propertyKey = new PropertyKey(readId(entry.id()), name);
        propertyKey.dataType(JsonUtil.fromJson(dataType, DataType.class));
        propertyKey.cardinality(JsonUtil.fromJson(cardinality,
                                                  Cardinality.class));
        propertyKey.properties(readIds(properties));
        readUserData(propertyKey, entry);
        return propertyKey;
    }

    @Override
    public BackendEntry writeIndexLabel(IndexLabel indexLabel) {
        TextBackendEntry entry = newBackendEntry(indexLabel);
        entry.column(HugeKeys.NAME, JsonUtil.toJson(indexLabel.name()));
        entry.column(HugeKeys.BASE_TYPE,
                     JsonUtil.toJson(indexLabel.baseType()));
        entry.column(HugeKeys.BASE_VALUE, writeId(indexLabel.baseValue()));
        entry.column(HugeKeys.INDEX_TYPE,
                     JsonUtil.toJson(indexLabel.indexType()));
        entry.column(HugeKeys.FIELDS,
                     writeIds(indexLabel.indexFields()));
        return entry;
    }

    @Override
    public IndexLabel readIndexLabel(BackendEntry backendEntry) {
        if (backendEntry == null) {
            return null;
        }

        backendEntry = convertEntry(backendEntry);
        assert backendEntry instanceof TextBackendEntry;

        TextBackendEntry entry = (TextBackendEntry) backendEntry;
        String name = JsonUtil.fromJson(entry.column(HugeKeys.NAME),
                                        String.class);
        String baseType = entry.column(HugeKeys.BASE_TYPE);
        String baseValue = entry.column(HugeKeys.BASE_VALUE);
        String indexType = entry.column(HugeKeys.INDEX_TYPE);
        String indexFields = entry.column(HugeKeys.FIELDS);

        IndexLabel indexLabel = new IndexLabel(readId(entry.id()), name);
        indexLabel.baseType(JsonUtil.fromJson(baseType, HugeType.class));
        indexLabel.baseValue(readId(baseValue));
        indexLabel.indexType(JsonUtil.fromJson(indexType, IndexType.class));
        indexLabel.indexFields(readIds(indexFields));
        return indexLabel;
    }

    private static String writeType(HugeType type) {
        return type.string();
    }

    private static String writeId(Id id) {
        if (id.number()) {
            return JsonUtil.toJson(id.asLong());
        } else {
            return JsonUtil.toJson(id.asString());
        }
    }

    private static Id readId(String id) {
        Object value = JsonUtil.fromJson(id, Object.class);
        if (value instanceof Number) {
            return IdGenerator.of(((Number) value).longValue());
        } else {
            assert value instanceof String;
            return IdGenerator.of(value.toString());
        }
    }

    private static Id readId(Id id) {
        return readId(id.asString());
    }

    private static String writeIds(Collection<Id> ids) {
        Object[] array = new Object[ids.size()];
        int i = 0;
        for (Id id : ids) {
            if (id.number()) {
                array[i++] = id.asLong();
            } else {
                array[i++] = id.asString();
            }
        }
        return JsonUtil.toJson(array);
    }

    private static Id[] readIds(String str) {
        Object[] values = JsonUtil.fromJson(str, Object[].class);
        Id[] ids = new Id[values.length];
        for (int i = 0; i < values.length; i++) {
            Object value = values[i];
            if (value instanceof Number) {
                ids[i] = IdGenerator.of(((Number) value).longValue());
            } else {
                assert value instanceof String;
                ids[i] = IdGenerator.of(value.toString());
            }
        }
        return ids;
    }

    private static void writeUserData(SchemaElement schema,
                                      TextBackendEntry entry) {
        entry.column(HugeKeys.USER_DATA, JsonUtil.toJson(schema.userData()));
    }

    private static void readUserData(SchemaElement schema,
                                     TextBackendEntry entry) {
        // Parse all user data of a schema element
        String userDataStr = entry.column(HugeKeys.USER_DATA);
        @SuppressWarnings("unchecked")
        Map<String, Object> userData = JsonUtil.fromJson(userDataStr,
                                                         Map.class);
        for (String key : userData.keySet()) {
            schema.userData(key, userData.get(key));
        }
    }
}
