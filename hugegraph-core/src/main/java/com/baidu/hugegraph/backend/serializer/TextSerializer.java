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
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.baidu.hugegraph.backend.serializer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.apache.tinkerpop.gremlin.structure.Direction;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.IdGeneratorFactory;
import com.baidu.hugegraph.backend.id.SplicingIdGenerator;
import com.baidu.hugegraph.backend.query.Condition;
import com.baidu.hugegraph.backend.query.ConditionQuery;
import com.baidu.hugegraph.backend.query.IdQuery;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.schema.HugeEdgeLabel;
import com.baidu.hugegraph.schema.HugeIndexLabel;
import com.baidu.hugegraph.schema.HugePropertyKey;
import com.baidu.hugegraph.schema.HugeVertexLabel;
import com.baidu.hugegraph.schema.SchemaElement;
import com.baidu.hugegraph.structure.HugeEdge;
import com.baidu.hugegraph.structure.HugeElement;
import com.baidu.hugegraph.structure.HugeIndex;
import com.baidu.hugegraph.structure.HugeProperty;
import com.baidu.hugegraph.structure.HugeVertex;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.Cardinality;
import com.baidu.hugegraph.type.define.DataType;
import com.baidu.hugegraph.type.define.EdgeLink;
import com.baidu.hugegraph.type.define.Frequency;
import com.baidu.hugegraph.type.define.HugeKeys;
import com.baidu.hugegraph.type.define.IndexType;
import com.baidu.hugegraph.type.schema.EdgeLabel;
import com.baidu.hugegraph.type.schema.IndexLabel;
import com.baidu.hugegraph.type.schema.PropertyKey;
import com.baidu.hugegraph.type.schema.VertexLabel;
import com.baidu.hugegraph.util.JsonUtil;
import com.google.common.collect.ImmutableList;

public class TextSerializer extends AbstractSerializer {

    private static final String COLUME_SPLITOR = SplicingIdGenerator.IDS_SPLITOR;
    private static final String VALUE_SPLITOR = "\u0004";

    public TextSerializer(final HugeGraph graph) {
        super(graph);
    }

    @Override
    public BackendEntry newBackendEntry(Id id) {
        return new TextBackendEntry(id);
    }

    @Override
    protected BackendEntry convertEntry(BackendEntry entry) {
        if (entry instanceof TextBackendEntry) {
            return entry;
        }

        TextBackendEntry text = new TextBackendEntry(entry.id());
        text.columns(entry.columns());
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
        PropertyKey pkey = this.graph.schema().propertyKey(colParts[1]);

        // Parse value
        Object value = JsonUtil.fromJson(colValue, pkey.clazz());

        // Set properties of vertex/edge
        if (pkey.cardinality() == Cardinality.SINGLE) {
            owner.addProperty(pkey.name(), value);
        } else {
            if (!(value instanceof Collection)) {
                throw new BackendException(
                          "Invalid value of non-sigle property: %s",
                          colValue);
            }
            for (Object v : (Collection<?>) value) {
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

        EdgeLabel label = this.graph.schema().edgeLabel(colParts[1]);

        // TODO: how to construct targetVertex with id
        Id otherVertexId = IdGeneratorFactory.generator().generate(colParts[3]);
        HugeVertex otherVertex = new HugeVertex(this.graph.graphTransaction(),
                                                otherVertexId, null);

        String[] valParts = colValue.split(VALUE_SPLITOR);
        Id id = IdGeneratorFactory.generator().generate(valParts[0]);

        HugeEdge edge = new HugeEdge(this.graph, id, label);

        boolean isOutEdge = colParts[0].equals(HugeType.EDGE_OUT.name());
        if (isOutEdge) {
            edge.targetVertex(otherVertex);
            vertex.addOutEdge(edge);
        } else {
            edge.sourceVertex(otherVertex);
            vertex.addInEdge(edge);
        }

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

        // Add all edges of a Vertex
        for (HugeEdge edge : vertex.getEdges()) {
            entry.column(this.formatEdgeName(edge),
                         this.formatEdgeValue(edge));
        }

        return entry;
    }

    @Override
    public HugeVertex readVertex(BackendEntry bytesEntry) {
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
            label = this.graph.schema().vertexLabel(labelName);
        }

        HugeVertex vertex = new HugeVertex(this.graph.graphTransaction(),
                                           entry.id(), label);

        // Parse all properties or edges of a Vertex
        for (String name : entry.columnNames()) {
            this.parseColumn(name, entry.column(name), vertex);
        }

        return vertex;
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
        Id id = IdGeneratorFactory.generator().generate(vertexLabel);

        TextBackendEntry entry = this.writeId(vertexLabel.type(), id);
        entry.column(HugeKeys.NAME.string(),
                     JsonUtil.toJson(vertexLabel.name()));
        entry.column(HugeKeys.PRIMARY_KEYS.string(),
                     JsonUtil.toJson(vertexLabel.primaryKeys().toArray()));
        entry.column(HugeKeys.INDEX_NAMES.string(),
                     JsonUtil.toJson(vertexLabel.indexNames().toArray()));
        writeProperties(vertexLabel, entry);
        return entry;
    }

    @Override
    public BackendEntry writeEdgeLabel(EdgeLabel edgeLabel) {
        Id id = IdGeneratorFactory.generator().generate(edgeLabel);

        TextBackendEntry entry = this.writeId(edgeLabel.type(), id);
        entry.column(HugeKeys.NAME.string(),
                     JsonUtil.toJson(edgeLabel.name()));
        entry.column(HugeKeys.FREQUENCY.string(),
                     JsonUtil.toJson(edgeLabel.frequency()));
        entry.column(HugeKeys.LINKS.string(),
                     JsonUtil.toJson(edgeLabel.links()));
        entry.column(HugeKeys.SORT_KEYS.string(),
                     JsonUtil.toJson(edgeLabel.sortKeys().toArray()));
        entry.column(HugeKeys.INDEX_NAMES.string(),
                     JsonUtil.toJson(edgeLabel.indexNames().toArray()));
        writeProperties(edgeLabel, entry);
        return entry;
    }

    @Override
    public BackendEntry writePropertyKey(PropertyKey propertyKey) {
        Id id = IdGeneratorFactory.generator().generate(propertyKey);

        TextBackendEntry entry = this.writeId(propertyKey.type(), id);
        entry.column(HugeKeys.NAME.string(),
                     JsonUtil.toJson(propertyKey.name()));
        entry.column(HugeKeys.DATA_TYPE.string(),
                     JsonUtil.toJson(propertyKey.dataType()));
        entry.column(HugeKeys.CARDINALITY.string(),
                     JsonUtil.toJson(propertyKey.cardinality()));
        writeProperties(propertyKey, entry);
        return entry;
    }

    public void writeProperties(SchemaElement schemaElement,
                                TextBackendEntry entry) {
        Set<String> properties = schemaElement.properties();
        if (properties == null) {
            entry.column(HugeKeys.PROPERTIES.string(), "[]");
        } else {
            entry.column(HugeKeys.PROPERTIES.string(),
                         JsonUtil.toJson(properties.toArray()));
        }
    }

    @Override
    public VertexLabel readVertexLabel(BackendEntry entry) {
        if (entry == null) {
            return null;
        }

        entry = convertEntry(entry);
        assert entry instanceof TextBackendEntry;

        TextBackendEntry textEntry = (TextBackendEntry) entry;
        String name = textEntry.column(HugeKeys.NAME.string());
        String properties = textEntry.column(HugeKeys.PROPERTIES.string());
        String primarykeys = textEntry.column(HugeKeys.PRIMARY_KEYS.string());
        String indexNames = textEntry.column(HugeKeys.INDEX_NAMES.string());

        HugeVertexLabel vertexLabel = new HugeVertexLabel(
                JsonUtil.fromJson(name, String.class));
        vertexLabel.properties(JsonUtil.fromJson(properties, String[].class));
        vertexLabel.primaryKeys(JsonUtil.fromJson(primarykeys, String[].class));
        vertexLabel.indexNames(JsonUtil.fromJson(indexNames, String[].class));

        return vertexLabel;
    }

    @Override
    public EdgeLabel readEdgeLabel(BackendEntry entry) {
        if (entry == null) {
            return null;
        }

        entry = convertEntry(entry);
        assert entry instanceof TextBackendEntry;

        TextBackendEntry textEntry = (TextBackendEntry) entry;
        String name = textEntry.column(HugeKeys.NAME.string());
        String frequency = textEntry.column(HugeKeys.FREQUENCY.string());
        String sortKeys = textEntry.column(HugeKeys.SORT_KEYS.string());
        String links = textEntry.column(HugeKeys.LINKS.string());
        String properties = textEntry.column(HugeKeys.PROPERTIES.string());
        String indexNames = textEntry.column(HugeKeys.INDEX_NAMES.string());

        HugeEdgeLabel edgeLabel = new HugeEdgeLabel(
                JsonUtil.fromJson(name, String.class));
        edgeLabel.frequency(JsonUtil.fromJson(frequency, Frequency.class));
        edgeLabel.links(JsonUtil.fromJson(links, EdgeLink[].class));
        edgeLabel.properties(JsonUtil.fromJson(properties, String[].class));
        edgeLabel.sortKeys(JsonUtil.fromJson(sortKeys, String[].class));
        edgeLabel.indexNames(JsonUtil.fromJson(indexNames, String[].class));
        return edgeLabel;
    }

    @Override
    public PropertyKey readPropertyKey(BackendEntry entry) {
        if (entry == null) {
            return null;
        }

        entry = convertEntry(entry);
        assert entry instanceof TextBackendEntry;

        TextBackendEntry textEntry = (TextBackendEntry) entry;
        String name = textEntry.column(HugeKeys.NAME.string());
        String dataType = textEntry.column(HugeKeys.DATA_TYPE.string());
        String cardinality = textEntry.column(HugeKeys.CARDINALITY.string());
        String properties = textEntry.column(HugeKeys.PROPERTIES.string());

        HugePropertyKey propertyKey = new HugePropertyKey(
                JsonUtil.fromJson(name, String.class));
        propertyKey.dataType(JsonUtil.fromJson(dataType, DataType.class));
        propertyKey.cardinality(JsonUtil.fromJson(cardinality,
                                Cardinality.class));
        propertyKey.properties(JsonUtil.fromJson(properties, String[].class));

        return propertyKey;
    }

    @Override
    public BackendEntry writeIndexLabel(IndexLabel indexLabel) {
        Id id = IdGeneratorFactory.generator().generate(indexLabel);
        TextBackendEntry entry = this.writeId(indexLabel.type(), id);

        entry.column(HugeKeys.NAME.string(),
                     JsonUtil.toJson(indexLabel.name()));
        entry.column(HugeKeys.BASE_TYPE.string(),
                     JsonUtil.toJson(indexLabel.baseType()));
        entry.column(HugeKeys.BASE_VALUE.string(),
                     JsonUtil.toJson(indexLabel.baseValue()));
        entry.column(HugeKeys.INDEX_TYPE.string(),
                     JsonUtil.toJson(indexLabel.indexType()));
        entry.column(HugeKeys.FIELDS.string(),
                     JsonUtil.toJson(indexLabel.indexFields().toArray()));
        return entry;
    }

    @Override
    public IndexLabel readIndexLabel(BackendEntry entry) {

        if (entry == null) {
            return null;
        }

        entry = convertEntry(entry);
        assert entry instanceof TextBackendEntry;

        TextBackendEntry textEntry = (TextBackendEntry) entry;
        String name = JsonUtil.fromJson(
                textEntry.column(HugeKeys.NAME.string()), String.class);
        HugeType baseType = JsonUtil.fromJson(
                textEntry.column(HugeKeys.BASE_TYPE.string()), HugeType.class);
        String baseValue = JsonUtil.fromJson(
                textEntry.column(HugeKeys.BASE_VALUE.string()), String.class);
        String indexType = textEntry.column(HugeKeys.INDEX_TYPE.string());
        String indexFields = textEntry.column(HugeKeys.FIELDS.string());

        HugeIndexLabel indexLabel = new HugeIndexLabel(name, baseType,
                                                       baseValue);
        indexLabel.indexType(JsonUtil.fromJson(indexType, IndexType.class));
        indexLabel.by(JsonUtil.fromJson(indexFields, String[].class));

        return indexLabel;
    }

    @Override
    public BackendEntry writeIndex(HugeIndex index) {

        Id id = IdGeneratorFactory.generator().generate(index.id());
        TextBackendEntry entry = new TextBackendEntry(id);
        // TODO: propertyValues may be a number (SEARCH index)
        entry.column(formatSyspropName(HugeKeys.PROPERTY_VALUES),
                     index.propertyValues().toString());
        entry.column(formatSyspropName(HugeKeys.INDEX_LABEL_NAME),
                     index.indexLabelName());
        // TODO: try to make these code more clear.
        Id[] ids = index.elementIds().toArray(new Id[0]);
        assert ids.length == 1;
        entry.column(formatSyspropName(HugeKeys.ELEMENT_IDS),
                     ids[0].asString());
        return entry;
    }

    @Override
    public HugeIndex readIndex(BackendEntry backendEntry) {
        if (backendEntry == null) {
            return null;
        }

        backendEntry = convertEntry(backendEntry);
        assert backendEntry instanceof TextBackendEntry;
        TextBackendEntry entry = (TextBackendEntry) backendEntry;

        String indexValues = entry.column(
                formatSyspropName(HugeKeys.PROPERTY_VALUES));
        String indexLabelName = entry.column(
                formatSyspropName(HugeKeys.INDEX_LABEL_NAME));
        String elementIds = entry.column(
                formatSyspropName(HugeKeys.ELEMENT_IDS));

        IndexLabel indexLabel = this.graph.schemaTransaction()
                                    .getIndexLabel(indexLabelName);

        HugeIndex index = new HugeIndex(indexLabel);
        index.propertyValues(indexValues);
        // TODO: don forget to remove the [] symbol
        String[] ids = JsonUtil.fromJson("[\"" + elementIds + "\"]",
                                         String[].class);
        for (String id : ids) {
            index.elementIds(IdGeneratorFactory.generator().generate(id));
        }

        return index;
    }
}
