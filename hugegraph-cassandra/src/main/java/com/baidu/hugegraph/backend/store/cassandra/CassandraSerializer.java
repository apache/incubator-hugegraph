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

package com.baidu.hugegraph.backend.store.cassandra;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import org.apache.tinkerpop.gremlin.structure.Direction;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.IdGeneratorFactory;
import com.baidu.hugegraph.backend.id.SplicingIdGenerator;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.serializer.AbstractSerializer;
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
import com.google.common.collect.ImmutableMap;

public class CassandraSerializer extends AbstractSerializer {

    public CassandraSerializer(final HugeGraph graph) {
        super(graph);
    }

    public CassandraBackendEntry newBackendEntry(HugeType type, Id id) {
        return new CassandraBackendEntry(type, id);
    }

    @Override
    public BackendEntry newBackendEntry(Id id) {
        return newBackendEntry(null, id);
    }

    protected CassandraBackendEntry newBackendEntry(HugeElement e) {
        return newBackendEntry(e.type(), e.id());
    }

    protected CassandraBackendEntry newBackendEntry(SchemaElement e) {
        Id id = IdGeneratorFactory.generator().generate(e);
        return newBackendEntry(e.type(), id);
    }

    protected CassandraBackendEntry newBackendEntry(HugeIndex index) {
        return newBackendEntry(index.type(), index.id());
    }

    @Override
    protected BackendEntry convertEntry(BackendEntry backendEntry) {
        if (!(backendEntry instanceof CassandraBackendEntry)) {
            throw new BackendException(
                    "CassandraSerializer just supports CassandraBackendEntry");
        }
        return backendEntry;
    }

    protected void parseProperty(String colName, String colValue,
                                 HugeElement owner) {
        // Get PropertyKey by PropertyKey name
        PropertyKey pkey = this.graph.schema().propertyKey(colName);

        // Parse value
        Object value = JsonUtil.fromJson(colValue, pkey.clazz());

        // Set properties of vertex/edge
        if (pkey.cardinality() == Cardinality.SINGLE) {
            owner.addProperty(pkey.name(), value);
        } else {
            if (!(value instanceof Collection)) {
                throw new BackendException(String.format(
                          "Invalid value of non-sigle property: %s",
                          colValue));
            }
            for (Object v : (Collection<?>) value) {
                owner.addProperty(pkey.name(), v);
            }
        }
    }

    protected CassandraBackendEntry.Row formatEdge(HugeEdge edge) {
        CassandraBackendEntry.Row row = new CassandraBackendEntry.Row(
                                        HugeType.EDGE, edge.id());
        // sourceVertex + direction + edge-label-name + sortValues + targetVertex
        row.column(HugeKeys.SOURCE_VERTEX, edge.owner().id().asString());
        row.column(HugeKeys.DIRECTION, edge.direction().name());
        row.column(HugeKeys.LABEL, edge.label());
        row.column(HugeKeys.SORT_VALUES, edge.name());
        row.column(HugeKeys.TARGET_VERTEX, edge.otherVertex().id().asString());

        if (!edge.hasProperties() && !edge.removed()) {
            row.column(HugeKeys.PROPERTIES, ImmutableMap.of());
        } else {
            // Format edge properties
            for (HugeProperty<?> prop : edge.getProperties().values()) {
                row.column(HugeKeys.PROPERTIES, prop.key(),
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
    protected HugeVertex parseEdge(CassandraBackendEntry.Row row,
                                   HugeVertex vertex) {
        String sourceVertexId = row.column(HugeKeys.SOURCE_VERTEX);
        Direction direction = Direction.valueOf(
                              row.column(HugeKeys.DIRECTION));
        String labelName = row.column(HugeKeys.LABEL);
        String sortValues = row.column(HugeKeys.SORT_VALUES);
        String targetVertexId = row.column(HugeKeys.TARGET_VERTEX);

        boolean isOutEdge = direction == Direction.OUT;
        EdgeLabel label = this.graph.schema().edgeLabel(labelName);

        Id vertexId = IdGeneratorFactory.generator().generate(targetVertexId);
        // TODO: improve Id parse()
        // TODO: get vertex-label from edge-label link
        String[] vIdParts = SplicingIdGenerator.parse(vertexId);
        VertexLabel vertexLabel = this.graph.schema().vertexLabel(vIdParts[0]);

        HugeVertex otherVertex = new HugeVertex(this.graph, vertexId,
                                                vertexLabel);
        otherVertex.name(vIdParts[1]);

        if (vertex == null) {
            Id id = IdGeneratorFactory.generator().generate(sourceVertexId);
            // TODO: get vertex-label from edge-label link
            vertex = new HugeVertex(this.graph, id, null);
        }

        HugeEdge edge = new HugeEdge(this.graph, null, label);

        if (isOutEdge) {
            edge.targetVertex(otherVertex);
            vertex.addOutEdge(edge);
            otherVertex.addInEdge(edge.switchOwner());
        } else {
            edge.sourceVertex(otherVertex);
            vertex.addInEdge(edge);
            otherVertex.addOutEdge(edge.switchOwner());
        }

        // Parse edge properties
        Map<String, String> props = row.column(HugeKeys.PROPERTIES);
        for (Map.Entry<String, String> prop : props.entrySet()) {
            this.parseProperty(prop.getKey(), prop.getValue(), edge);
        }

        edge.name(sortValues);
        edge.assignId();

        return vertex;
    }

    @Override
    public BackendEntry writeVertex(HugeVertex vertex) {
        CassandraBackendEntry entry = newBackendEntry(vertex);

        boolean changed = vertex.hasProperties() || vertex.removed();
        entry.selfChanged(changed);

        entry.column(HugeKeys.LABEL, vertex.label());
        entry.column(HugeKeys.PRIMARY_VALUES, vertex.name());

        // Add all properties of a Vertex
        for (HugeProperty<?> prop : vertex.getProperties().values()) {
            entry.column(HugeKeys.PROPERTIES, prop.key(),
                         JsonUtil.toJson(prop.value()));
        }

        return entry;
    }

    @Override
    public HugeVertex readVertex(BackendEntry bytesEntry) {
        if (bytesEntry == null) {
            return null;
        }
        bytesEntry = this.convertEntry(bytesEntry);
        assert bytesEntry instanceof CassandraBackendEntry;
        CassandraBackendEntry entry = (CassandraBackendEntry) bytesEntry;

        String labelName = entry.column(HugeKeys.LABEL);
        String name = entry.column(HugeKeys.PRIMARY_VALUES);

        VertexLabel label = this.graph.schema().vertexLabel(labelName);

        HugeVertex vertex = new HugeVertex(this.graph, entry.id(), label);
        vertex.name(name);

        // Parse all properties of a Vertex
        Map<String, String> props = entry.column(HugeKeys.PROPERTIES);
        for (Map.Entry<String, String> prop : props.entrySet()) {
            this.parseProperty(prop.getKey(), prop.getValue(), vertex);
        }

        // Parse all edges of a Vertex
        for (CassandraBackendEntry.Row edge : entry.subRows()) {
            this.parseEdge(edge, vertex);
        }
        return vertex;
    }

    @Override
    public BackendEntry writeEdge(HugeEdge edge) {
        return new CassandraBackendEntry(this.formatEdge(edge));
    }

    @Override
    public HugeEdge readEdge(BackendEntry bytesEntry) {
        if (bytesEntry == null) {
            return null;
        }
        bytesEntry = this.convertEntry(bytesEntry);
        assert bytesEntry instanceof CassandraBackendEntry;
        CassandraBackendEntry entry = (CassandraBackendEntry) bytesEntry;

        Set<HugeEdge> edges = this.parseEdge(entry.row(), null).getEdges();
        assert edges.size() == 1;
        return edges.iterator().next();
    }

    @Override
    public BackendEntry writeId(HugeType type, Id id) {
        // NOTE: Cassandra does not need to add type prefix for id
        return newBackendEntry(type, id);
    }

    @Override
    public Query writeQuery(Query query) {
        // NOTE: Cassandra does not need to add type prefix for id
        return query;
    }

    @Override
    public BackendEntry writeVertexLabel(VertexLabel vertexLabel) {
        CassandraBackendEntry entry = newBackendEntry(vertexLabel);
        entry.column(HugeKeys.NAME, vertexLabel.name());
        entry.column(HugeKeys.PRIMARY_KEYS,
                     JsonUtil.toJson(vertexLabel.primaryKeys().toArray()));
        entry.column(HugeKeys.INDEX_NAMES,
                     JsonUtil.toJson(vertexLabel.indexNames().toArray()));
        writeProperties(vertexLabel, entry);
        return entry;
    }

    @Override
    public BackendEntry writeEdgeLabel(EdgeLabel edgeLabel) {
        CassandraBackendEntry entry = newBackendEntry(edgeLabel);
        entry.column(HugeKeys.NAME, edgeLabel.name());
        entry.column(HugeKeys.FREQUENCY,
                     JsonUtil.toJson(edgeLabel.frequency()));
        entry.column(HugeKeys.LINKS,
                     JsonUtil.toJson(edgeLabel.links()));
        entry.column(HugeKeys.SORT_KEYS,
                     JsonUtil.toJson(edgeLabel.sortKeys().toArray()));
        entry.column(HugeKeys.INDEX_NAMES,
                     JsonUtil.toJson(edgeLabel.indexNames().toArray()));
        writeProperties(edgeLabel, entry);
        return entry;
    }

    @Override
    public BackendEntry writePropertyKey(PropertyKey propertyKey) {
        CassandraBackendEntry entry = newBackendEntry(propertyKey);
        entry.column(HugeKeys.NAME, propertyKey.name());
        entry.column(HugeKeys.DATA_TYPE,
                     JsonUtil.toJson(propertyKey.dataType()));
        entry.column(HugeKeys.CARDINALITY,
                     JsonUtil.toJson(propertyKey.cardinality()));
        writeProperties(propertyKey, entry);
        return entry;
    }

    public void writeProperties(SchemaElement schemaElement,
                                CassandraBackendEntry entry) {
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
        assert backendEntry instanceof CassandraBackendEntry;

        CassandraBackendEntry entry = (CassandraBackendEntry) backendEntry;
        String name = entry.column(HugeKeys.NAME);
        String properties = entry.column(HugeKeys.PROPERTIES);
        String primarykeys = entry.column(HugeKeys.PRIMARY_KEYS);
        String indexNames = entry.column(HugeKeys.INDEX_NAMES);

        HugeVertexLabel vertexLabel = new HugeVertexLabel(name);
        vertexLabel.properties(JsonUtil.fromJson(properties, String[].class));
        vertexLabel.primaryKeys(JsonUtil.fromJson(primarykeys,
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
        assert backendEntry instanceof CassandraBackendEntry;

        CassandraBackendEntry entry = (CassandraBackendEntry) backendEntry;
        String name = entry.column(HugeKeys.NAME);
        String frequency = entry.column(HugeKeys.FREQUENCY);
        String links = entry.column(HugeKeys.LINKS);
        String sortKeys = entry.column(HugeKeys.SORT_KEYS);
        String properties = entry.column(HugeKeys.PROPERTIES);
        String indexNames = entry.column(HugeKeys.INDEX_NAMES);

        HugeEdgeLabel edgeLabel = new HugeEdgeLabel(name);
        edgeLabel.frequency(JsonUtil.fromJson(frequency, Frequency.class));
        edgeLabel.links(JsonUtil.fromJson(links, EdgeLink[].class));
        edgeLabel.properties(JsonUtil.fromJson(properties, String[].class));
        edgeLabel.sortKeys(JsonUtil.fromJson(sortKeys, String[].class));
        edgeLabel.indexNames(JsonUtil.fromJson(indexNames, String[].class));
        return edgeLabel;
    }

    @Override
    public PropertyKey readPropertyKey(BackendEntry backendEntry) {
        if (backendEntry == null) {
            return null;
        }

        backendEntry = convertEntry(backendEntry);
        assert backendEntry instanceof CassandraBackendEntry;

        CassandraBackendEntry entry = (CassandraBackendEntry) backendEntry;
        String name = entry.column(HugeKeys.NAME);
        String dataType = entry.column(HugeKeys.DATA_TYPE);
        String cardy = entry.column(HugeKeys.CARDINALITY);
        String properties = entry.column(HugeKeys.PROPERTIES);

        HugePropertyKey propertyKey = new HugePropertyKey(name);
        propertyKey.dataType(JsonUtil.fromJson(dataType, DataType.class));
        propertyKey.cardinality(JsonUtil.fromJson(cardy, Cardinality.class));
        propertyKey.properties(JsonUtil.fromJson(properties, String[].class));

        return propertyKey;
    }

    @Override
    public BackendEntry writeIndexLabel(IndexLabel indexLabel) {
        CassandraBackendEntry entry = newBackendEntry(indexLabel);
        entry.column(HugeKeys.NAME, indexLabel.name());
        entry.column(HugeKeys.BASE_TYPE,
                     JsonUtil.toJson(indexLabel.baseType()));
        entry.column(HugeKeys.BASE_VALUE, indexLabel.baseValue());
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
        assert backendEntry instanceof CassandraBackendEntry;

        CassandraBackendEntry entry = (CassandraBackendEntry) backendEntry;
        String indexName = entry.column(HugeKeys.NAME);
        HugeType baseType = JsonUtil.fromJson(
                entry.column(HugeKeys.BASE_TYPE), HugeType.class);
        String baseValue = entry.column(HugeKeys.BASE_VALUE);
        String indexType = entry.column(HugeKeys.INDEX_TYPE);
        String indexFields = entry.column(HugeKeys.FIELDS);

        HugeIndexLabel indexLabel = new HugeIndexLabel(indexName, baseType,
                                                       baseValue);
        indexLabel.indexType(JsonUtil.fromJson(indexType, IndexType.class));
        indexLabel.by(JsonUtil.fromJson(indexFields, String[].class));

        return indexLabel;
    }

    @Override
    public BackendEntry writeIndex(HugeIndex index) {
        CassandraBackendEntry entry = newBackendEntry(index);
        /*
         * When field-values is null and elementIds size is 0, it is
         * meaningful for deletion of index data in secondary/search index.
         */
        if (index.fieldValues() == null && index.elementIds().size() == 0) {
            entry.column(HugeKeys.INDEX_LABEL_NAME, index.indexLabelName());
        } else {
            entry.column(HugeKeys.FIELD_VALUES, index.fieldValues());
            entry.column(HugeKeys.INDEX_LABEL_NAME, index.indexLabelName());
            // TODO: try to make these code more clear.
            Id[] ids = index.elementIds().toArray(new Id[0]);
            assert ids.length == 1;
            entry.column(HugeKeys.ELEMENT_IDS, ids[0].asString());
        }
        return entry;
    }

    @Override
    public HugeIndex readIndex(BackendEntry backendEntry) {
        if (backendEntry == null) {
            return null;
        }

        backendEntry = convertEntry(backendEntry);
        assert backendEntry instanceof CassandraBackendEntry;
        CassandraBackendEntry entry = (CassandraBackendEntry) backendEntry;

        Object indexValues = entry.column(HugeKeys.FIELD_VALUES);
        String indexLabelName = entry.column(HugeKeys.INDEX_LABEL_NAME);
        String elementIds = entry.column(HugeKeys.ELEMENT_IDS);

        IndexLabel indexLabel = this.graph.schemaTransaction()
                                .getIndexLabel(indexLabelName);

        HugeIndex index = new HugeIndex(indexLabel);
        index.fieldValues(indexValues);

        String[] ids = JsonUtil.fromJson(elementIds, String[].class);
        for (String id : ids) {
            index.elementIds(IdGeneratorFactory.generator().generate(id));
        }

        return index;
    }
}
