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

import java.util.Collection;
import java.util.List;

import org.apache.commons.lang.NotImplementedException;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.id.EdgeId;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.backend.query.Condition;
import com.baidu.hugegraph.backend.query.Condition.Relation;
import com.baidu.hugegraph.backend.query.ConditionQuery;
import com.baidu.hugegraph.backend.query.IdQuery;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.serializer.BinaryBackendEntry.BinaryId;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.backend.store.BackendEntry.BackendColumn;
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
import com.baidu.hugegraph.type.define.Directions;
import com.baidu.hugegraph.type.define.HugeKeys;
import com.baidu.hugegraph.util.Bytes;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.KryoUtil;
import com.baidu.hugegraph.util.StringEncoding;
import com.baidu.hugegraph.util.StringUtil;

public class BinarySerializer extends AbstractSerializer {

    private static final byte[] EMPTY_BYTES = new byte[0];

    private final boolean keyWithIdPrefix;

    public BinarySerializer() {
        this(true);
    }

    public BinarySerializer(boolean keyWithIdPrefix) {
        this.keyWithIdPrefix = keyWithIdPrefix;
    }

    @Override
    public BinaryBackendEntry newBackendEntry(HugeType type, Id id) {
        BytesBuffer buffer = BytesBuffer.allocate(1 + id.length());
        BinaryId bid = new BinaryId(buffer.writeId(id).bytes(), id);
        return new BinaryBackendEntry(type, bid);
    }

    private BinaryBackendEntry newBackendEntry(HugeVertex vertex) {
        return newBackendEntry(vertex.type(), vertex.id());
    }

    private BinaryBackendEntry newBackendEntry(HugeEdge edge) {
        BinaryId id = new BinaryId(formatEdgeName(edge),
                                   edge.idWithDirection());
        return new BinaryBackendEntry(edge.type(), id);
    }

    @SuppressWarnings("unused")
    private BinaryBackendEntry newBackendEntry(SchemaElement elem) {
        return newBackendEntry(elem.type(), elem.id());
    }

    @Override
    protected BinaryBackendEntry convertEntry(BackendEntry entry) {
        assert entry instanceof BinaryBackendEntry;
        return (BinaryBackendEntry) entry;
    }

    protected byte[] formatSyspropName(Id id, HugeKeys col) {
        int idLen = this.keyWithIdPrefix ? 1 + id.length() : 0;
        BytesBuffer buffer = BytesBuffer.allocate(idLen + 1 + 1);
        byte sysprop = HugeType.SYS_PROPERTY.code();
        if (this.keyWithIdPrefix) {
            buffer.writeId(id);
        }
        return buffer.write(sysprop).write(col.code()).bytes();
    }

    protected byte[] formatSyspropName(BinaryId id, HugeKeys col) {
        int idLen = this.keyWithIdPrefix ? id.length() : 0;
        BytesBuffer buffer = BytesBuffer.allocate(idLen + 1 + 1);
        byte sysprop = HugeType.SYS_PROPERTY.code();
        if (this.keyWithIdPrefix) {
            buffer.write(id.asBytes());
        }
        return buffer.write(sysprop).write(col.code()).bytes();
    }

    protected BackendColumn formatLabel(HugeElement elem) {
        BackendColumn col = new BackendColumn();
        col.name = this.formatSyspropName(elem.id(), HugeKeys.LABEL);
        Id label = elem.schemaLabel().id();
        BytesBuffer buffer = BytesBuffer.allocate(label.length() + 1);
        col.value = buffer.writeId(label).bytes();
        return col;
    }

    protected byte[] formatPropertyName(HugeProperty<?> prop) {
        Id id = prop.element().id();
        int idLen = this.keyWithIdPrefix ? 1 + id.length() : 0;
        Id pkeyId = prop.propertyKey().id();
        BytesBuffer buffer = BytesBuffer.allocate(idLen + 2 + pkeyId.length());
        if (this.keyWithIdPrefix) {
            buffer.writeId(id);
        }
        buffer.write(prop.type().code());
        buffer.writeId(pkeyId);
        return buffer.bytes();
    }

    protected BackendColumn formatProperty(HugeProperty<?> prop) {
        return BackendColumn.of(this.formatPropertyName(prop),
                                KryoUtil.toKryo(prop.value()));
    }

    protected void parseProperty(Id pkeyId, byte[] val, HugeElement owner) {
        PropertyKey pkey = owner.graph().propertyKey(pkeyId);

        // Parse value
        Object value = KryoUtil.fromKryo(val, pkey.clazz());

        // Set properties of vertex/edge
        if (pkey.cardinality() == Cardinality.SINGLE) {
            owner.addProperty(pkey, value);
        } else {
            if (!(value instanceof Collection)) {
                throw new BackendException(
                          "Invalid value of non-single property: %s", value);
            }
            for (Object v : (Collection<?>) value) {
                owner.addProperty(pkey, v);
            }
        }
    }

    protected byte[] formatEdgeName(HugeEdge edge) {
        // owner-vertex + dir + edge-label + sort-values + other-vertex

        BytesBuffer buffer = BytesBuffer.allocate(256);

        buffer.writeId(edge.ownerVertex().id());
        buffer.write(edge.type().code());
        buffer.writeId(edge.schemaLabel().id());
        buffer.writeString(edge.name()); // TODO: write if need
        buffer.writeId(edge.otherVertex().id());

        return buffer.bytes();
    }

    protected byte[] formatEdgeValue(HugeEdge edge) {
        int propsCount = edge.getProperties().size();
        BytesBuffer buffer = BytesBuffer.allocate(4 + 16 * propsCount);

        // Write edge id
        //buffer.writeId(edge.id());

        // Write edge properties size
        buffer.writeInt(propsCount);

        // Write edge properties data
        for (HugeProperty<?> property : edge.getProperties().values()) {
            buffer.writeId(property.propertyKey().id());
            buffer.writeBytes(KryoUtil.toKryo(property.value()));
        }

        return buffer.bytes();
    }

    protected BackendColumn formatEdge(HugeEdge edge) {
        byte[] name;
        if (this.keyWithIdPrefix) {
            name = this.formatEdgeName(edge);
        } else {
            name = EMPTY_BYTES;
        }
        return BackendColumn.of(name, this.formatEdgeValue(edge));
    }

    protected void parseEdge(BackendColumn col, HugeVertex vertex,
                             HugeGraph graph) {
        // owner-vertex + dir + edge-label + sort-values + other-vertex

        BytesBuffer buffer = BytesBuffer.wrap(col.name);
        if (this.keyWithIdPrefix) {
            // Consume owner-vertex id
            buffer.readId();
        }
        byte type = buffer.read();
        Id labelId = buffer.readId();
        String sk = buffer.readString();
        Id otherVertexId = buffer.readId();

        boolean isOutEdge = (type == HugeType.EDGE_OUT.code());
        EdgeLabel edgeLabel = graph.edgeLabel(labelId);
        VertexLabel srcLabel = graph.vertexLabel(edgeLabel.sourceLabel());
        VertexLabel tgtLabel = graph.vertexLabel(edgeLabel.targetLabel());

        HugeVertex otherVertex;
        if (isOutEdge) {
            vertex.vertexLabel(srcLabel);
            otherVertex = new HugeVertex(graph, otherVertexId, tgtLabel);
        } else {
            vertex.vertexLabel(tgtLabel);
            otherVertex = new HugeVertex(graph, otherVertexId, srcLabel);
        }

        HugeEdge edge = new HugeEdge(graph, null, edgeLabel);
        edge.name(sk);

        if (isOutEdge) {
            edge.vertices(vertex, vertex, otherVertex);
            edge.assignId();
            vertex.addOutEdge(edge);
            otherVertex.addInEdge(edge.switchOwner());
        } else {
            edge.vertices(vertex, otherVertex, vertex);
            edge.assignId();
            vertex.addInEdge(edge);
            otherVertex.addOutEdge(edge.switchOwner());
        }

        vertex.propNotLoaded();
        otherVertex.propNotLoaded();

        // Write edge-id + edge-properties
        buffer = BytesBuffer.wrap(col.value);

        //Id id = buffer.readId();

        // Write edge properties
        int size = buffer.readInt();
        for (int i = 0; i < size; i++) {
            this.parseProperty(buffer.readId(), buffer.readBytes(), edge);
        }
    }

    protected void parseColumn(BackendColumn col, HugeVertex vertex) {
        BytesBuffer buffer = BytesBuffer.wrap(col.name);
        Id id = this.keyWithIdPrefix ? buffer.readId() : vertex.id();
        E.checkState(buffer.remaining() > 0, "Missing column type");
        byte type = buffer.read();
        // Parse property
        if (type == HugeType.PROPERTY.code()) {
            Id pkeyId = buffer.readId();
            this.parseProperty(pkeyId, col.value, vertex);
        }
        // Parse edge
        else if (type == HugeType.EDGE_IN.code() ||
                 type == HugeType.EDGE_OUT.code()) {
            this.parseEdge(col, vertex, vertex.graph());
        }
        // Parse system property
        else if (type == HugeType.SYS_PROPERTY.code()) {
            // pass
        }
        // Invalid entry
        else {
            E.checkState(false, "Invalid entry(%s) with unknown type(%s): 0x%s",
                         id, type & 0xff, Bytes.toHex(col.name));
        }
    }

    protected byte[] formatIndexName(HugeIndex index) {
        Id indexId = index.id();
        if (indexIdLengthExceedLimit(indexId)) {
            indexId = index.hashId();
        }
        Id elemId = index.elementId();
        BytesBuffer buffer = BytesBuffer.allocate(1 + indexId.length() +
                                                  1 + elemId.length());
        // Write index-id + element-id
        buffer.writeId(indexId);
        buffer.writeId(elemId, true);

        return buffer.bytes();
    }

    protected void parseIndexName(BinaryBackendEntry entry, HugeIndex index,
                                  Object fieldValues) {
        for (BackendColumn col : entry.columns()) {
            if (indexFieldValuesUnmatched(col.value, fieldValues)) {
                // Skip if field-values don't matched (just the same hash)
                continue;
            }
            BytesBuffer buffer = BytesBuffer.wrap(col.name);
            buffer.readId();
            index.elementIds(buffer.readId(true));
        }
    }

    @Override
    public BackendEntry writeVertex(HugeVertex vertex) {
        BinaryBackendEntry entry = newBackendEntry(vertex);

        if (vertex.removed()) {
            return entry;
        }

        // Write vertex label
        entry.column(this.formatLabel(vertex));

        // Write all properties of a Vertex
        for (HugeProperty<?> prop : vertex.getProperties().values()) {
            entry.column(this.formatProperty(prop));
        }

        return entry;
    }

    @Override
    public BackendEntry writeVertexProperty(HugeVertexProperty<?> prop) {
        BinaryBackendEntry entry = newBackendEntry(prop.element());
        entry.column(this.formatProperty(prop));
        entry.subId(IdGenerator.of(prop.key()));
        return entry;
    }

    @Override
    public HugeVertex readVertex(HugeGraph graph, BackendEntry bytesEntry) {
        if (bytesEntry == null) {
            return null;
        }
        BinaryBackendEntry entry = this.convertEntry(bytesEntry);

        // Parse label
        final byte[] VL = this.formatSyspropName(entry.id(), HugeKeys.LABEL);
        BackendColumn vl = entry.column(VL);
        VertexLabel label = VertexLabel.NONE;
        if (vl != null) {
            label = graph.vertexLabel(BytesBuffer.wrap(vl.value).readId());
        }

        // Parse id
        Id id = entry.id().origin();
        HugeVertex vertex = new HugeVertex(graph, id, label);

        // Parse all properties and edges of a Vertex
        for (BackendColumn col : entry.columns()) {
            this.parseColumn(col, vertex);
        }

        return vertex;
    }

    @Override
    public BackendEntry writeEdge(HugeEdge edge) {
        BinaryBackendEntry entry = newBackendEntry(edge);
        entry.column(this.formatEdge(edge));
        return entry;
    }

    @Override
    public BackendEntry writeEdgeProperty(HugeEdgeProperty<?> prop) {
        // TODO: entry.column(this.formatProperty(prop));
        throw new NotImplementedException("Unsupported writeEdgeProperty()");
    }

    @Override
    public HugeEdge readEdge(HugeGraph graph, BackendEntry entry) {
        throw new NotImplementedException("Unsupported readEdge()");
    }

    @Override
    public BackendEntry writeIndex(HugeIndex index) {
        BinaryBackendEntry entry;
        if (index.fieldValues() == null && index.elementIds().size() == 0) {
            /*
             * When field-values is null and elementIds size is 0, it is
             * meaningful for deletion of index data in secondary/range index.
             * TODO: improve
             */
            Id id = index.indexLabel();
            entry = newBackendEntry(index.type(), id);
        } else {
            Id id = index.id();
            byte[] value = null;
            if (!index.type().isRangeIndex() && indexIdLengthExceedLimit(id)) {
                id = index.hashId();
                // Save field-values as column value if the key is a hash string
                value = StringEncoding.encode(index.fieldValues().toString());
            }

            entry = newBackendEntry(index.type(), id);
            entry.column(this.formatIndexName(index), value);
            entry.subId(index.elementId());
        }
        return entry;
    }

    @Override
    public HugeIndex readIndex(HugeGraph graph, ConditionQuery query,
                               BackendEntry bytesEntry) {
        if (bytesEntry == null) {
            return null;
        }

        BinaryBackendEntry entry = this.convertEntry(bytesEntry);
        // NOTE: index id without length prefix
        byte[] bytes = entry.id().asBytes(1);
        HugeIndex index = HugeIndex.parseIndexId(graph, entry.type(), bytes);

        Object fieldValues = null;
        if (!index.type().isRangeIndex()) {
            fieldValues = query.condition(HugeKeys.FIELD_VALUES);
            if (!index.fieldValues().equals(fieldValues)) {
                // Update field-values for hashed index-id
                index.fieldValues(fieldValues);
            }
        }

        this.parseIndexName(entry, index, fieldValues);
        return index;
    }

    @Override
    public BackendEntry writeId(HugeType type, Id id) {
        return newBackendEntry(type, id);
    }

    @Override
    protected Id writeQueryId(HugeType type, Id id) {
        if (type.isEdge()) {
            id = writeEdgeId(id);
        } else {
            BytesBuffer buffer = BytesBuffer.allocate(1 + id.length());
            id = new BinaryId(buffer.writeId(id).bytes(), id);
        }
        return id;
    }

    @Override
    protected Id writeQueryEdgeCondition(Query query) {
        int count = 0;
        BytesBuffer buffer = BytesBuffer.allocate(256);
        for (HugeKeys key : EdgeId.KEYS) {
            Object value = ((ConditionQuery) query).condition(key);

            if (value != null) {
                count++;
            } else {
                if (key == HugeKeys.DIRECTION) {
                    value = Directions.OUT;
                } else {
                    break;
                }
            }

            if (key == HugeKeys.OWNER_VERTEX ||
                key == HugeKeys.OTHER_VERTEX) {
                Id id = HugeVertex.getIdValue(value);
                buffer.writeId(id);
            } else if (key == HugeKeys.DIRECTION) {
                byte t = ((Directions) value).type().code();
                buffer.write(t);
            } else if (key == HugeKeys.LABEL) {
                assert value instanceof Id;
                buffer.writeId((Id) value);
            } else if (key == HugeKeys.SORT_VALUES) {
                assert value instanceof String;
                buffer.writeString((String) value);
            } else {
                assert false : key;
            }
        }

        if (count > 0) {
            assert count == query.conditions().size();
            return new BinaryId(buffer.bytes(), null);
        }

        return null;
    }

    @Override
    protected Query writeQueryCondition(Query query) {
        HugeType type = query.resultType();
        if (!type.isIndex()) {
            return query;
        }

        ConditionQuery cq = (ConditionQuery) query;

        // Convert secondary-index query to id query
        if (type.isSecondaryIndex()) {
            return this.writeSecondaryIndexQuery(cq);
        }

        // Convert range-index query to id range query
        if (type.isRangeIndex()) {
            return this.writeRangeIndexQuery(cq);
        }

        E.checkState(false, "Unsupported index query: %s", type);
        return null;
    }

    private Query writeSecondaryIndexQuery(ConditionQuery query) {
        E.checkArgument(query.allSysprop() &&
                        query.conditions().size() == 2,
                        "There should be two conditions: " +
                        "INDEX_LABEL_ID and FIELD_VALUES" +
                        "in secondary index query");

        Id index = (Id) query.condition(HugeKeys.INDEX_LABEL_ID);
        Object key = query.condition(HugeKeys.FIELD_VALUES);

        E.checkArgument(index != null, "Please specify the index label");
        E.checkArgument(key != null, "Please specify the index key");

        Id id = formatIndexId(query.resultType(), index, key);
        return new IdQuery(query, id);
    }

    private Query writeRangeIndexQuery(ConditionQuery query) {
        Id index = (Id) query.condition(HugeKeys.INDEX_LABEL_ID);
        E.checkArgument(index != null,
                        "Please specify the index label");

        List<Condition> fields = query.syspropConditions(HugeKeys.FIELD_VALUES);
        E.checkArgument(!fields.isEmpty(),
                        "Please specify the index field values");

        Object keyEq = null;
        Object keyMin = null;
        boolean keyMinEq = false;
        Object keyMax = null;
        boolean keyMaxEq = false;

        for (Condition c : fields) {
            Relation r = (Relation) c;
            switch (r.relation()) {
                case EQ:
                    keyEq = r.value();
                    break;
                case GTE:
                    keyMinEq = true;
                case GT:
                    keyMin = r.value();
                    break;
                case LTE:
                    keyMaxEq = true;
                case LT:
                    keyMax = r.value();
                    break;
                default:
                    E.checkArgument(false, "Unsupported relation '%s'",
                                    r.relation());
            }
        }

        HugeType type = query.resultType();
        if (keyEq != null) {
            Id id = formatIndexId(type, index, keyEq);
            return new IdQuery(query, id);
        }

        if (keyMin == null) {
            E.checkArgument(keyMax != null,
                            "Please specify at least one condition");
            // Set keyMin to 0
            keyMin = StringUtil.valueOf(keyMax.getClass(), "0");
            keyMinEq = true;
        }

        query = query.copy();
        query.resetConditions();

        Id min = formatIndexId(type, index, keyMin);
        if (keyMinEq) {
            query.gte(HugeKeys.ID, min);
        } else {
            query.gt(HugeKeys.ID, min);
        }

        if (keyMax == null) {
            Id prefix = formatIndexId(type, index, null);
            // Reset the first byte to make same length-prefix
            prefix.asBytes()[0] = min.asBytes()[0];
            query.prefix(HugeKeys.ID, prefix);
        } else {
            Id max = formatIndexId(type, index, keyMax);
            if (keyMaxEq) {
                query.lte(HugeKeys.ID, max);
            } else {
                query.lt(HugeKeys.ID, max);
            }
        }
        return query;
    }

    private static BinaryId writeEdgeId(Id id) {
        EdgeId edgeId;
        if (id instanceof EdgeId) {
            edgeId = (EdgeId) id;
        } else {
            edgeId = EdgeId.parse(id.asString());
        }
        BytesBuffer buffer = BytesBuffer.allocate(256);
        buffer.writeId(edgeId.ownerVertexId());
        buffer.write(edgeId.direction().type().code());
        buffer.writeId(edgeId.edgeLabelId());
        buffer.writeString(edgeId.sortValues());
        buffer.writeId(edgeId.otherVertexId());
        return new BinaryId(buffer.bytes(), id);
    }

    protected static BinaryId formatIndexId(HugeType type, Id indexLabel,
                                            Object fieldValues) {
        Id id = HugeIndex.formatIndexId(type, indexLabel, fieldValues);
        if (indexIdLengthExceedLimit(id)) {
            id = HugeIndex.formatIndexHashId(type, indexLabel, fieldValues);
        }
        BytesBuffer buffer = BytesBuffer.allocate(1 + id.length());
        return new BinaryId(buffer.writeId(id).bytes(), id);
    }

    protected static boolean indexIdLengthExceedLimit(Id id) {
        return id.asBytes().length > BytesBuffer.INDEX_ID_MAX_LENGTH;
    }

    protected static boolean indexFieldValuesUnmatched(byte[] value,
                                                       Object fieldValues) {
        if (value != null && value.length > 0 && fieldValues != null) {
            if (!StringEncoding.decode(value).equals(fieldValues)) {
                return true;
            }
        }
        return false;
    }

    // TODO: remove these methods when improving schema serialize
    private static String splitKeyId(byte[] bytes) {
        BytesBuffer buffer = BytesBuffer.wrap(bytes);
        buffer.readId();
        return buffer.readStringFromRemaining();
    }

    private static byte[] joinIdKey(Id id, String key) {
        int size = 1 + id.length() + key.length();
        BytesBuffer buffer = BytesBuffer.allocate(size);
        buffer.writeId(id);
        buffer.writeStringToRemaining(key);
        return buffer.bytes();
    }

    private BackendEntry text2bin(BackendEntry entry) {
        Id id = IdGenerator.of(entry.id().asLong());
        BinaryBackendEntry bin = newBackendEntry(entry.type(), id);
        TextBackendEntry text = (TextBackendEntry) entry;
        for (String name : text.columnNames()) {
            String value = text.column(name);
            bin.column(joinIdKey(id, name),
                       StringEncoding.encode(value));
        }
        return bin;
    }

    private BackendEntry bin2text(BackendEntry entry) {
        if (entry == null) {
            return null;
        }
        BinaryBackendEntry bin = (BinaryBackendEntry) entry;
        Id id = IdGenerator.of(bin.id().origin().asString());
        TextBackendEntry text = new TextBackendEntry(null, id);
        for (BackendColumn col : bin.columns()) {
            String name = splitKeyId(col.name);
            String value = StringEncoding.decode(col.value);
            text.column(name, value);
        }
        return text;
    }

    // TODO: improve schema serialize
    private final TextSerializer textSerializer = new TextSerializer();

    @Override
    public BackendEntry writeVertexLabel(VertexLabel vertexLabel) {
        // TODO Auto-generated method stub
        return text2bin(this.textSerializer.writeVertexLabel(vertexLabel));
    }

    @Override
    public VertexLabel readVertexLabel(HugeGraph graph, BackendEntry entry) {
        // TODO Auto-generated method stub
        return this.textSerializer.readVertexLabel(graph, bin2text(entry));
    }

    @Override
    public BackendEntry writeEdgeLabel(EdgeLabel edgeLabel) {
        // TODO Auto-generated method stub
        return text2bin(this.textSerializer.writeEdgeLabel(edgeLabel));
    }

    @Override
    public EdgeLabel readEdgeLabel(HugeGraph graph, BackendEntry entry) {
        // TODO Auto-generated method stub
        return this.textSerializer.readEdgeLabel(graph, bin2text(entry));
    }

    @Override
    public BackendEntry writePropertyKey(PropertyKey propertyKey) {
        // TODO Auto-generated method stub
        return text2bin(this.textSerializer.writePropertyKey(propertyKey));
    }

    @Override
    public PropertyKey readPropertyKey(HugeGraph graph, BackendEntry entry) {
        // TODO Auto-generated method stub
        return this.textSerializer.readPropertyKey(graph, bin2text(entry));
    }

    @Override
    public BackendEntry writeIndexLabel(IndexLabel indexLabel) {
        // TODO Auto-generated method stub
        return text2bin(this.textSerializer.writeIndexLabel(indexLabel));
    }

    @Override
    public IndexLabel readIndexLabel(HugeGraph graph, BackendEntry entry) {
        // TODO Auto-generated method stub
        return this.textSerializer.readIndexLabel(graph, bin2text(entry));
    }
}
