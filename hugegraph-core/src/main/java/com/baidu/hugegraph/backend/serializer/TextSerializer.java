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
import java.util.stream.Collectors;

import org.apache.commons.lang.NotImplementedException;
import org.apache.tinkerpop.gremlin.structure.Direction;

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
import com.baidu.hugegraph.schema.EdgeLabel;
import com.baidu.hugegraph.schema.IndexLabel;
import com.baidu.hugegraph.schema.PropertyKey;
import com.baidu.hugegraph.schema.SchemaElement;
import com.baidu.hugegraph.schema.VertexLabel;
import com.baidu.hugegraph.structure.HugeEdge;
import com.baidu.hugegraph.structure.HugeElement;
import com.baidu.hugegraph.structure.HugeIndex;
import com.baidu.hugegraph.structure.HugeProperty;
import com.baidu.hugegraph.structure.HugeVertex;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.Cardinality;
import com.baidu.hugegraph.type.define.DataType;
import com.baidu.hugegraph.type.define.Frequency;
import com.baidu.hugegraph.type.define.HugeKeys;
import com.baidu.hugegraph.type.define.IdStrategy;
import com.baidu.hugegraph.type.define.IndexType;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.JsonUtil;
import com.google.common.collect.ImmutableList;

public class TextSerializer extends AbstractSerializer {

    private static final String COLUME_SPLITOR = TextBackendEntry.COLUME_SPLITOR;
    private static final String VALUE_SPLITOR = TextBackendEntry.VALUE_SPLITOR;

    @Override
    public BackendEntry newBackendEntry(Id id) {
        return new TextBackendEntry(id);
    }

    @Override
    protected BackendEntry convertEntry(BackendEntry backendEntry) {
        if (backendEntry instanceof TextBackendEntry) {
            return backendEntry;
        }

        TextBackendEntry text = new TextBackendEntry(backendEntry.id());
        text.columns(backendEntry.columns());
        return text;
    }

    protected String formatSyspropName(String name) {
        return String.format("%s%s%s", HugeType.SYS_PROPERTY.name(),
                             COLUME_SPLITOR, name);
    }

    protected String formatSyspropName(HugeKeys col) {
        return this.formatSyspropName(col.string());
    }

    protected Object formatPropertyName(Object key) {
        return String.format("%s%s%s", HugeType.PROPERTY.name(),
                             COLUME_SPLITOR, key);
    }

    protected String formatPropertyName(HugeProperty<?> prop) {
        return String.format("%s%s%s", prop.type().name(),
                             COLUME_SPLITOR, prop.key());
    }

    protected String formatPropertyValue(HugeProperty<?> prop) {
        // May be a single value or a list of values
        return JsonUtil.toJson(prop.value());
    }

    protected void parseProperty(String colName, String colValue,
                                 HugeElement owner) {
        String[] colParts = colName.split(COLUME_SPLITOR);
        assert colParts.length == 2 : colName;

        // Get PropertyKey by PropertyKey name
        PropertyKey pkey = owner.graph().propertyKey(colParts[1]);

        // Parse value
        Object value = JsonUtil.fromJson(colValue, pkey.clazz());

        // Set properties of vertex/edge
        if (pkey.cardinality() == Cardinality.SINGLE) {
            owner.addProperty(pkey.name(), value);
        } else {
            if (!(value instanceof Collection)) {
                throw new BackendException(
                          "Invalid value of non-sigle property: %s", colValue);
            }
            for (Object v : (Collection<?>) value) {
                v = JsonUtil.castNumber(v, pkey.dataType().clazz());
                owner.addProperty(pkey.name(), v);
            }
        }
    }

    protected String formatEdgeName(HugeEdge edge) {
        // Edge name: type + edge-label-name + sortKeys + targetVertex
        StringBuilder sb = new StringBuilder(256);
        sb.append(edge.type().name());
        sb.append(COLUME_SPLITOR);
        sb.append(edge.label());
        sb.append(COLUME_SPLITOR);
        sb.append(edge.name());
        sb.append(COLUME_SPLITOR);
        sb.append(edge.otherVertex().id().asString());
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
        String[] colParts = colName.split(COLUME_SPLITOR);

        HugeGraph graph = vertex.graph();
        EdgeLabel label = graph.edgeLabel(colParts[1]);

        // TODO: how to construct targetVertex with id
        Id otherVertexId = IdGenerator.of(colParts[3]);
        HugeVertex otherVertex = new HugeVertex(graph, otherVertexId, null);

        String[] valParts = colValue.split(VALUE_SPLITOR);
        Id id = IdGenerator.of(valParts[0]);

        HugeEdge edge = new HugeEdge(graph, id, label);
        edge.name(colParts[2]);

        boolean isOutEdge = colParts[0].equals(HugeType.EDGE_OUT.name());
        if (isOutEdge) {
            edge.targetVertex(otherVertex);
            vertex.addOutEdge(edge);
        } else {
            edge.sourceVertex(otherVertex);
            vertex.addInEdge(edge);
        }

        otherVertex.propLoaded(false);

        // Edge properties
        for (int i = 1; i < valParts.length; i += 2) {
            this.parseProperty(valParts[i], valParts[i + 1], edge);
        }
    }

    protected void parseColumn(String colName, String colValue,
                               HugeVertex vertex) {
        // Column name
        String type = colName.split(COLUME_SPLITOR, 2)[0];
        // Parse property
        if (type.equals(HugeType.PROPERTY.name())) {
            this.parseProperty(colName, colValue, vertex);
        }
        // Parse edge
        if (type.equals(HugeType.EDGE_OUT.name()) ||
            type.equals(HugeType.EDGE_IN.name())) {
            this.parseEdge(colName, colValue, vertex);
        }
    }

    @Override
    public BackendEntry writeVertex(HugeVertex vertex) {
        TextBackendEntry entry = new TextBackendEntry(vertex.id());

        // Write label (NOTE: maybe just with edges if label is null)
        if (vertex.vertexLabel() != null) {
            entry.column(this.formatSyspropName(HugeKeys.LABEL),
                         vertex.label());
        }

        // Add all properties of a Vertex
        for (HugeProperty<?> prop : vertex.getProperties().values()) {
            entry.column(this.formatPropertyName(prop),
                         this.formatPropertyValue(prop));
        }

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
        String labelName = entry.column(this.formatSyspropName(HugeKeys.LABEL));
        VertexLabel label = null;
        if (labelName != null) {
            label = graph.vertexLabel(labelName);
        }

        HugeVertex vertex = new HugeVertex(graph, entry.id(), label);

        // Parse all properties or edges of a Vertex
        for (String name : entry.columnNames()) {
            this.parseColumn(name, entry.column(name), vertex);
        }

        return vertex;
    }

    @Override
    public BackendEntry writeEdge(HugeEdge edge) {
        TextBackendEntry entry = new TextBackendEntry(edge.owner().id());
        entry.column(this.formatEdgeName(edge), this.formatEdgeValue(edge));
        return entry;
    }

    @Override
    public HugeEdge readEdge(BackendEntry backendEntry, HugeGraph graph) {
        E.checkNotNull(graph, "serializer graph");
        // TODO: implement
        throw new NotImplementedException("Unsupport readEdge()");
    }

    @Override
    public BackendEntry writeIndex(HugeIndex index) {
        TextBackendEntry entry = new TextBackendEntry(index.id());
        /*
         * When field-values is null and elementIds size is 0, it is
         * meaningful for deletion of index data in secondary/search index.
         */
        if (index.fieldValues() == null && index.elementIds().size() == 0) {
            entry.column(formatSyspropName(HugeKeys.INDEX_LABEL_NAME),
                         index.indexLabelName());
        } else {
            // TODO: field-values may be a number (SEARCH index)
            entry.column(formatSyspropName(HugeKeys.FIELD_VALUES),
                         index.fieldValues().toString());
            entry.column(formatSyspropName(HugeKeys.INDEX_LABEL_NAME),
                         index.indexLabelName());
            Set<String> ids = index.elementIds().stream().map(id ->
                              id.asString()).collect(Collectors.toSet());
            entry.column(formatSyspropName(HugeKeys.ELEMENT_IDS),
                         JsonUtil.toJson(ids));
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
        String indexLabelName = entry.column(
                formatSyspropName(HugeKeys.INDEX_LABEL_NAME));
        String elementIds = entry.column(
                formatSyspropName(HugeKeys.ELEMENT_IDS));

        IndexLabel indexLabel = graph.indexLabel(indexLabelName);

        HugeIndex index = new HugeIndex(indexLabel);
        index.fieldValues(indexValues);
        String[] ids = JsonUtil.fromJson(elementIds, String[].class);
        for (String id : ids) {
            index.elementIds(IdGenerator.of(id));
        }

        return index;
    }

    @Override
    public TextBackendEntry writeId(HugeType type, Id id) {
        if (SchemaElement.isSchema(type)) {
            id = id.prefixWith(type);
        }
        return new TextBackendEntry(id);
    }

    @Override
    public Query writeQuery(Query query) {
        /*
         * Serialize edge query by id/conditions to query by src-vertex +
         * edge-name.
         */
        if (query.resultType() == HugeType.EDGE && query instanceof IdQuery) {
            return this.writeEdgeQuery((IdQuery) query);
        }

        // Prefix schema-id with type
        if (SchemaElement.isSchema(query.resultType()) &&
            query instanceof IdQuery) {
            // Serialize query id of schema
            IdQuery result = (IdQuery) query.clone();
            result.resetIds();
            for (Id id : query.ids()) {
                result.query(id.prefixWith(query.resultType()));
            }
            return result;
        }

        // Serialize query key
        if (!query.conditions().isEmpty() && query instanceof ConditionQuery) {
            ConditionQuery result = (ConditionQuery) query;
            // No user-prop when serialize
            assert result.allSysprop();
            for (Condition.Relation r : result.relations()) {
                // Serialize and reset key
                r.key(formatSyspropName((HugeKeys) r.key()));
                // Serialize has-key
                if (r.relation() == Condition.RelationType.CONTAINS_KEY) {
                    r.value(formatPropertyName(r.value()));
                }
            }
        }

        return query;
    }

    protected IdQuery writeEdgeQuery(IdQuery query) {
        IdQuery result = (IdQuery) query.clone();
        result.resetIds();

        if (!query.conditions().isEmpty() && !query.ids().isEmpty()) {
            throw new BackendException("Not supported query edge by id(s) " +
                                       "and condition(s) at the same time");
        }

        // By id
        if (query.ids().size() > 0) {
            for (Id id : query.ids()) {
                // TODO: improve edge id split
                List<String> idParts = new ArrayList<>(ImmutableList.copyOf(
                                           SplicingIdGenerator.split(id)));
                /*
                 * Note that we assume the id without Direction if it contains
                 * 4 parts.
                 */
                if (idParts.size() == 4) {
                    // Ensure edge id with Direction
                    idParts.add(1, HugeType.EDGE_OUT.name());
                }

                result.query(SplicingIdGenerator.concat(
                             idParts.toArray(new String[0])));
            }
            return result;
        }

        // By condition (then convert the query to query by id)
        List<String> condParts = new ArrayList<>(query.conditions().size());

        HugeKeys[] keys = new HugeKeys[] {
                HugeKeys.SOURCE_VERTEX,
                HugeKeys.DIRECTION,
                HugeKeys.LABEL,
                HugeKeys.SORT_VALUES,
                HugeKeys.TARGET_VERTEX
        };

        for (HugeKeys key : keys) {
            Object value = ((ConditionQuery) query).condition(key);
            if (value == null) {
                break;
            }
            // Serialize value
            if (value instanceof Direction) {
                value = ((Direction) value) == Direction.OUT ?
                        HugeType.EDGE_OUT.name() :
                        HugeType.EDGE_IN.name();
            }
            condParts.add(value.toString());

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

    @Override
    public BackendEntry writeVertexLabel(VertexLabel vertexLabel) {
        Id id = IdGenerator.of(vertexLabel);

        TextBackendEntry entry = this.writeId(vertexLabel.type(), id);
        entry.column(HugeKeys.NAME, JsonUtil.toJson(vertexLabel.name()));
        entry.column(HugeKeys.ID_STRATEGY,
                     JsonUtil.toJson(vertexLabel.idStrategy()));
        entry.column(HugeKeys.PRIMARY_KEYS,
                     JsonUtil.toJson(vertexLabel.primaryKeys().toArray()));
        entry.column(HugeKeys.NULLABLE_KEYS,
                     JsonUtil.toJson(vertexLabel.nullableKeys().toArray()));
        entry.column(HugeKeys.INDEX_NAMES,
                     JsonUtil.toJson(vertexLabel.indexNames().toArray()));
        writeProperties(vertexLabel, entry);
        return entry;
    }

    @Override
    public BackendEntry writeEdgeLabel(EdgeLabel edgeLabel) {
        Id id = IdGenerator.of(edgeLabel);

        TextBackendEntry entry = this.writeId(edgeLabel.type(), id);
        entry.column(HugeKeys.NAME, JsonUtil.toJson(edgeLabel.name()));
        entry.column(HugeKeys.SOURCE_LABEL,
                     JsonUtil.toJson(edgeLabel.sourceLabel()));
        entry.column(HugeKeys.TARGET_LABEL,
                     JsonUtil.toJson(edgeLabel.targetLabel()));
        entry.column(HugeKeys.FREQUENCY,
                     JsonUtil.toJson(edgeLabel.frequency()));
        entry.column(HugeKeys.SORT_KEYS,
                     JsonUtil.toJson(edgeLabel.sortKeys().toArray()));
        entry.column(HugeKeys.NULLABLE_KEYS,
                     JsonUtil.toJson(edgeLabel.nullableKeys().toArray()));
        entry.column(HugeKeys.INDEX_NAMES,
                     JsonUtil.toJson(edgeLabel.indexNames().toArray()));
        writeProperties(edgeLabel, entry);
        return entry;
    }

    @Override
    public BackendEntry writePropertyKey(PropertyKey propertyKey) {
        Id id = IdGenerator.of(propertyKey);

        TextBackendEntry entry = this.writeId(propertyKey.type(), id);
        entry.column(HugeKeys.NAME, JsonUtil.toJson(propertyKey.name()));
        entry.column(HugeKeys.DATA_TYPE,
                     JsonUtil.toJson(propertyKey.dataType()));
        entry.column(HugeKeys.CARDINALITY,
                     JsonUtil.toJson(propertyKey.cardinality()));
        writeProperties(propertyKey, entry);
        return entry;
    }

    public void writeProperties(SchemaElement schemaElement,
                                TextBackendEntry entry) {
        Set<String> properties = schemaElement.properties();
        if (properties == null) {
            entry.column(HugeKeys.PROPERTIES, "[]");
        } else {
            entry.column(HugeKeys.PROPERTIES,
                         JsonUtil.toJson(properties.toArray()));
        }
    }

    @Override
    public VertexLabel readVertexLabel(BackendEntry backendEntry) {
        if (backendEntry == null) {
            return null;
        }

        backendEntry = convertEntry(backendEntry);
        assert backendEntry instanceof TextBackendEntry;

        TextBackendEntry entry = (TextBackendEntry) backendEntry;
        String name = entry.column(HugeKeys.NAME);
        String idStrategy = entry.column(HugeKeys.ID_STRATEGY);
        String properties = entry.column(HugeKeys.PROPERTIES);
        String primarykeys = entry.column(HugeKeys.PRIMARY_KEYS);
        String nullablekeys = entry.column(HugeKeys.NULLABLE_KEYS);
        String indexNames = entry.column(HugeKeys.INDEX_NAMES);

        VertexLabel vertexLabel = new VertexLabel(JsonUtil.fromJson(
                                                  name, String.class));
        vertexLabel.idStrategy(JsonUtil.fromJson(idStrategy, IdStrategy.class));
        vertexLabel.properties(JsonUtil.fromJson(properties, String[].class));
        vertexLabel.primaryKeys(JsonUtil.fromJson(primarykeys, String[].class));
        vertexLabel.nullableKeys(JsonUtil.fromJson(nullablekeys,
                                                   String[].class));
        vertexLabel.indexNames(JsonUtil.fromJson(indexNames, String[].class));

        return vertexLabel;
    }

    @Override
    public EdgeLabel readEdgeLabel(BackendEntry backendEntry) {
        if (backendEntry == null) {
            return null;
        }

        backendEntry = convertEntry(backendEntry);
        assert backendEntry instanceof TextBackendEntry;

        TextBackendEntry entry = (TextBackendEntry) backendEntry;
        String name = entry.column(HugeKeys.NAME);
        String sourceLabel = entry.column(HugeKeys.SOURCE_LABEL);
        String targetLabel = entry.column(HugeKeys.TARGET_LABEL);
        String frequency = entry.column(HugeKeys.FREQUENCY);
        String sortKeys = entry.column(HugeKeys.SORT_KEYS);
        String nullablekeys = entry.column(HugeKeys.NULLABLE_KEYS);
        String properties = entry.column(HugeKeys.PROPERTIES);
        String indexNames = entry.column(HugeKeys.INDEX_NAMES);

        EdgeLabel edgeLabel = new EdgeLabel(JsonUtil.fromJson(
                                            name, String.class));
        edgeLabel.sourceLabel(JsonUtil.fromJson(sourceLabel, String.class));
        edgeLabel.targetLabel(JsonUtil.fromJson(targetLabel, String.class));
        edgeLabel.frequency(JsonUtil.fromJson(frequency, Frequency.class));
        edgeLabel.properties(JsonUtil.fromJson(properties, String[].class));
        edgeLabel.sortKeys(JsonUtil.fromJson(sortKeys, String[].class));
        edgeLabel.nullableKeys(JsonUtil.fromJson(nullablekeys, String[].class));
        edgeLabel.indexNames(JsonUtil.fromJson(indexNames, String[].class));
        return edgeLabel;
    }

    @Override
    public PropertyKey readPropertyKey(BackendEntry backendEntry) {
        if (backendEntry == null) {
            return null;
        }

        backendEntry = convertEntry(backendEntry);
        assert backendEntry instanceof TextBackendEntry;

        TextBackendEntry entry = (TextBackendEntry) backendEntry;
        String name = entry.column(HugeKeys.NAME);
        String dataType = entry.column(HugeKeys.DATA_TYPE);
        String cardinality = entry.column(HugeKeys.CARDINALITY);
        String properties = entry.column(HugeKeys.PROPERTIES);

        PropertyKey propertyKey = new PropertyKey(JsonUtil.fromJson(
                                                  name, String.class));
        propertyKey.dataType(JsonUtil.fromJson(dataType, DataType.class));
        propertyKey.cardinality(JsonUtil.fromJson(cardinality,
                                Cardinality.class));
        propertyKey.properties(JsonUtil.fromJson(properties, String[].class));

        return propertyKey;
    }

    @Override
    public BackendEntry writeIndexLabel(IndexLabel indexLabel) {
        Id id = IdGenerator.of(indexLabel);
        TextBackendEntry entry = this.writeId(indexLabel.type(), id);

        entry.column(HugeKeys.NAME,
                     JsonUtil.toJson(indexLabel.name()));
        entry.column(HugeKeys.BASE_TYPE,
                     JsonUtil.toJson(indexLabel.baseType()));
        entry.column(HugeKeys.BASE_VALUE,
                     JsonUtil.toJson(indexLabel.baseValue()));
        entry.column(HugeKeys.INDEX_TYPE,
                     JsonUtil.toJson(indexLabel.indexType()));
        entry.column(HugeKeys.FIELDS,
                     JsonUtil.toJson(indexLabel.indexFields().toArray()));
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
        HugeType baseType = JsonUtil.fromJson(entry.column(HugeKeys.BASE_TYPE),
                                              HugeType.class);
        String baseValue = JsonUtil.fromJson(entry.column(HugeKeys.BASE_VALUE),
                                             String.class);
        String indexType = entry.column(HugeKeys.INDEX_TYPE);
        String indexFields = entry.column(HugeKeys.FIELDS);

        IndexLabel indexLabel = new IndexLabel(name, baseType, baseValue);
        indexLabel.indexType(JsonUtil.fromJson(indexType, IndexType.class));
        indexLabel.indexFields(JsonUtil.fromJson(indexFields, String[].class));

        return indexLabel;
    }
}
