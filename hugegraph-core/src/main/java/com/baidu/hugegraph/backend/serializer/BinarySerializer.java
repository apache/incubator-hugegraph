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
import java.util.Map;

import org.apache.commons.lang.NotImplementedException;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.id.EdgeId;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.backend.page.PageState;
import com.baidu.hugegraph.backend.query.Condition;
import com.baidu.hugegraph.backend.query.Condition.Relation;
import com.baidu.hugegraph.backend.query.ConditionQuery;
import com.baidu.hugegraph.backend.query.IdPrefixQuery;
import com.baidu.hugegraph.backend.query.IdRangeQuery;
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
import com.baidu.hugegraph.type.define.DataType;
import com.baidu.hugegraph.type.define.Directions;
import com.baidu.hugegraph.type.define.Frequency;
import com.baidu.hugegraph.type.define.HugeKeys;
import com.baidu.hugegraph.type.define.IdStrategy;
import com.baidu.hugegraph.type.define.IndexType;
import com.baidu.hugegraph.type.define.SchemaStatus;
import com.baidu.hugegraph.type.define.SerialEnum;
import com.baidu.hugegraph.util.Bytes;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.JsonUtil;
import com.baidu.hugegraph.util.KryoUtil;
import com.baidu.hugegraph.util.NumericUtil;
import com.baidu.hugegraph.util.StringEncoding;

public class BinarySerializer extends AbstractSerializer {

    public static final byte[] EMPTY_BYTES = new byte[0];

    /*
     * Id is stored in column name if keyWithIdPrefix=true like RocksDB,
     * else stored in rowkey like HBase.
     */
    private final boolean keyWithIdPrefix;
    private final boolean indexWithIdPrefix;

    public BinarySerializer() {
        this(true, true);
    }

    public BinarySerializer(boolean keyWithIdPrefix,
                            boolean indexWithIdPrefix) {
        this.keyWithIdPrefix = keyWithIdPrefix;
        this.indexWithIdPrefix = indexWithIdPrefix;
    }

    @Override
    public BinaryBackendEntry newBackendEntry(HugeType type, Id id) {
        BytesBuffer buffer = BytesBuffer.allocate(1 + id.length());
        BinaryId bid = new BinaryId(buffer.writeId(id).bytes(), id);
        return new BinaryBackendEntry(type, bid);
    }

    protected final BinaryBackendEntry newBackendEntry(HugeVertex vertex) {
        return newBackendEntry(vertex.type(), vertex.id());
    }

    protected final BinaryBackendEntry newBackendEntry(HugeEdge edge) {
        BinaryId id = new BinaryId(formatEdgeName(edge),
                                   edge.idWithDirection());
        return new BinaryBackendEntry(edge.type(), id);
    }

    protected final BinaryBackendEntry newBackendEntry(SchemaElement elem) {
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
        Object value = KryoUtil.fromKryo(val, pkey.implementClazz());

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

    protected void formatProperties(Collection<HugeProperty<?>> props,
                                    BytesBuffer buffer) {
        // Write properties size
        buffer.writeInt(props.size());

        // Write properties data
        for (HugeProperty<?> property : props) {
            buffer.writeId(property.propertyKey().id());
            buffer.writeBytes(KryoUtil.toKryo(property.value()));
        }
    }

    protected void parseProperties(BytesBuffer buffer, HugeElement owner) {
        int size = buffer.readInt();
        for (int i = 0; i < size; i++) {
            this.parseProperty(buffer.readId(), buffer.readBytes(), owner);
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

        // Write edge properties
        this.formatProperties(edge.getProperties().values(), buffer);

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

        // Parse edge-id + edge-properties
        buffer = BytesBuffer.wrap(col.value);

        //Id id = buffer.readId();

        // Parse edge properties
        this.parseProperties(buffer, edge);
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
        Id elemId = index.elementId();
        int idLen = 1 + elemId.length();

        BytesBuffer buffer;
        if (!this.indexWithIdPrefix) {
            buffer = BytesBuffer.allocate(idLen);
        } else {
            Id indexId = index.id();
            if (indexIdLengthExceedLimit(indexId)) {
                indexId = index.hashId();
            }
            // Write index-id
            idLen += 1 + indexId.length();
            buffer = BytesBuffer.allocate(idLen);
            buffer.writeId(indexId);
        }

        // Write element-id
        buffer.writeId(elemId, true);

        return buffer.bytes();
    }

    protected void parseIndexName(BinaryBackendEntry entry, HugeIndex index,
                                  Object fieldValues) {
        for (BackendColumn col : entry.columns()) {
            if (indexFieldValuesUnmatched(col.value, fieldValues)) {
                // Skip if field-values is not matched (just the same hash)
                continue;
            }
            BytesBuffer buffer = BytesBuffer.wrap(col.name);
            if (this.indexWithIdPrefix) {
                buffer.readId();
            }
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
             * meaningful for deletion of index data by index label.
             * TODO: improve
             */
            entry = this.formatILDeletion(index);
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
    protected Query writeQueryEdgeCondition(Query query) {
        ConditionQuery cq = (ConditionQuery) query;
        if (cq.hasRangeCondition()) {
            return this.writeQueryEdgeRangeCondition(cq);
        } else {
            return this.writeQueryEdgePrefixCondition(cq);
        }
    }

    private Query writeQueryEdgeRangeCondition(ConditionQuery cq) {
        List<Condition> sortValues = cq.syspropConditions(HugeKeys.SORT_VALUES);
        E.checkArgument(sortValues.size() >= 1 && sortValues.size() <= 2,
                        "Edge range query must be with sort-values range");
        // Would ignore target vertex
        Id vertex = cq.condition(HugeKeys.OWNER_VERTEX);
        Directions direction = cq.condition(HugeKeys.DIRECTION);
        if (direction == null) {
            direction = Directions.OUT;
        }
        Id label = cq.condition(HugeKeys.LABEL);

        int size = 1 + vertex.length() + 1 + label.length() + 16;
        BytesBuffer start = BytesBuffer.allocate(size);
        start.writeId(vertex);
        start.write(direction.type().code());
        start.writeId(label);

        BytesBuffer end = BytesBuffer.allocate(size);
        end.copyFrom(start);

        int minEq = -1;
        int maxEq = -1;
        for (Condition sortValue : sortValues) {
            Condition.Relation r = (Condition.Relation) sortValue;
            switch (r.relation()) {
                case GTE:
                    minEq = 1;
                    start.writeString((String) r.value());
                    break;
                case GT:
                    minEq = 0;
                    start.writeString((String) r.value());
                    break;
                case LTE:
                    maxEq = 1;
                    end.writeString((String) r.value());
                    break;
                case LT:
                    maxEq = 0;
                    end.writeString((String) r.value());
                    break;
                default:
                    E.checkArgument(false, "Unsupported relation '%s'",
                                    r.relation());
            }
        }

        // Sort-value will be empty if there is no start sort-value
        Id startId = new BinaryId(start.bytes(), null);
        // Set endId as prefix if there is no end sort-value
        Id endId = new BinaryId(end.bytes(), null);
        if (maxEq == -1) {
            return new IdPrefixQuery(cq, startId, minEq == 1, endId);
        }
        return new IdRangeQuery(cq, startId, minEq == 1, endId, maxEq == 1);
    }

    private Query writeQueryEdgePrefixCondition(ConditionQuery cq) {
        int count = 0;
        BytesBuffer buffer = BytesBuffer.allocate(64);
        for (HugeKeys key : EdgeId.KEYS) {
            Object value = cq.condition(key);

            if (value != null) {
                count++;
            } else {
                if (key == HugeKeys.DIRECTION) {
                    // Direction is null, set to OUT
                    value = Directions.OUT;
                } else {
                    break;
                }
            }

            if (key == HugeKeys.OWNER_VERTEX ||
                key == HugeKeys.OTHER_VERTEX) {
                buffer.writeId((Id) value);
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
            assert count == cq.conditions().size();
            return new IdPrefixQuery(cq, new BinaryId(buffer.bytes(), null));
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

        // Convert secondary-index or search-index query to id query
        if (type.isStringIndex()) {
            return this.writeStringIndexQuery(cq);
        }

        // Convert range-index query to id range query
        if (type.isRangeIndex()) {
            return this.writeRangeIndexQuery(cq);
        }

        E.checkState(false, "Unsupported index query: %s", type);
        return null;
    }

    private Query writeStringIndexQuery(ConditionQuery query) {
        E.checkArgument(query.allSysprop() &&
                        query.conditions().size() == 2,
                        "There should be two conditions: " +
                        "INDEX_LABEL_ID and FIELD_VALUES" +
                        "in secondary index query");

        Id index = query.condition(HugeKeys.INDEX_LABEL_ID);
        Object key = query.condition(HugeKeys.FIELD_VALUES);

        E.checkArgument(index != null, "Please specify the index label");
        E.checkArgument(key != null, "Please specify the index key");

        Id prefix = formatIndexId(query.resultType(), index, key);

        /*
         * If used paging and the page number is not empty, deserialize
         * the page to id and use it as the starting row for this query
         */
        Query newQuery;
        if (query.paging() && !query.page().isEmpty()) {
            byte[] position = PageState.fromString(query.page()).position();
            E.checkArgument(Bytes.compare(position, prefix.asBytes()) >= 0,
                            "Invalid page out of lower bound");
            BinaryId start = new BinaryId(position, null);
            newQuery = new IdPrefixQuery(query, start, prefix);
        } else {
            newQuery = new IdPrefixQuery(query, prefix);
        }
        return newQuery;
    }

    private Query writeRangeIndexQuery(ConditionQuery query) {
        Id index = query.condition(HugeKeys.INDEX_LABEL_ID);
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
            return new IdPrefixQuery(query, id);
        }

        if (keyMin == null) {
            E.checkArgument(keyMax != null,
                            "Please specify at least one condition");
            // Set keyMin to min value
            keyMin = NumericUtil.minValueOf(keyMax.getClass());
            keyMinEq = true;
        }

        Id min = formatIndexId(type, index, keyMin);
        if (!keyMinEq) {
            /*
             * Increase 1 to keyMin, index GT query is a scan with GT prefix,
             * inclusiveStart=false will also match index started with keyMin
             */
            increaseOne(min.asBytes());
            keyMinEq = true;
        }

        Id start = min;
        if (query.paging() && !query.page().isEmpty()) {
            byte[] position = PageState.fromString(query.page()).position();
            E.checkArgument(Bytes.compare(position, start.asBytes()) >= 0,
                            "Invalid page out of lower bound");
            start = new BinaryId(position, null);
        }

        Query newQuery;
        if (keyMax == null) {
            Id prefix = formatIndexId(type, index, null);
            // Reset the first byte to make same length-prefix
            prefix.asBytes()[0] = min.asBytes()[0];
            newQuery = new IdPrefixQuery(query, start, keyMinEq, prefix);
        } else {
            Id max = formatIndexId(type, index, keyMax);
            if (keyMaxEq) {
                keyMaxEq = false;
                increaseOne(max.asBytes());
            }
            newQuery = new IdRangeQuery(query, start, keyMinEq, max, keyMaxEq);
        }
        return newQuery;
    }

    private BinaryBackendEntry formatILDeletion(HugeIndex index) {
        Id id = index.indexLabel();
        BinaryBackendEntry entry = newBackendEntry(index.type(), id);
        switch (index.type()) {
            case SECONDARY_INDEX:
            case SEARCH_INDEX:
                String idString = id.asString();
                int idLength = idString.length();
                // TODO: to improve, use BytesBuffer to generate a mask
                for (int i = idLength - 1; i < 128; i++) {
                    BytesBuffer buffer = BytesBuffer.allocate(idLength + 1);
                    /*
                     * Index id type is always non-number, that it will prefix
                     * with '0b1xxx xxxx'
                     */
                    buffer.writeUInt8(i | 0x80);
                    buffer.write(IdGenerator.of(idString).asBytes());
                    entry.column(buffer.bytes(), null);
                }
                break;
            case RANGE_INDEX:
                int il = (int) id.asLong();
                for (int i = 0; i < 4; i++) {
                    /*
                     * Field value(Number type) length is 1, 2, 4, 8.
                     * Index label id length is 4
                     */
                    int length = (int) Math.pow(2, i) + 4;
                    length -= 1;
                    BytesBuffer buffer = BytesBuffer.allocate(1 + 4);
                    buffer.writeUInt8(length | 0x80);
                    buffer.writeInt(il);
                    entry.column(buffer.bytes(), null);
                }
                break;
            default:
                throw new AssertionError(String.format(
                          "Index type must be Secondary or Range, " +
                          "but got '%s'", index.type()));
        }
        return entry;
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

    public static final byte[] increaseOne(byte[] bytes) {
        final byte BYTE_MAX_VALUE = (byte) 0xff;
        assert bytes.length > 0;
        byte last = bytes[bytes.length - 1];
        if (last != BYTE_MAX_VALUE) {
            bytes[bytes.length - 1] += 0x01;
        } else {
            // Process overflow (like [1, 255] => [2, 0])
            int i = bytes.length - 1;
            for (; i > 0 && bytes[i] == BYTE_MAX_VALUE; --i) {
                bytes[i] += 0x01;
            }
            if (bytes[i] == BYTE_MAX_VALUE) {
                assert i == 0;
                throw new BackendException("Unable to increase bytes: %s",
                                           Bytes.toHex(bytes));
            }
            bytes[i] += 0x01;
        }
        return bytes;
    }

    @Override
    public BackendEntry writeVertexLabel(VertexLabel vertexLabel) {
        SchemaSerializer serializer = new SchemaSerializer();
        return serializer.writeVertexLabel(vertexLabel);
    }

    @Override
    public VertexLabel readVertexLabel(HugeGraph graph,
                                       BackendEntry backendEntry) {
        if (backendEntry == null) {
            return null;
        }
        BinaryBackendEntry entry = this.convertEntry(backendEntry);

        SchemaSerializer serializer = new SchemaSerializer();
        return serializer.readVertexLabel(graph, entry);
    }

    @Override
    public BackendEntry writeEdgeLabel(EdgeLabel edgeLabel) {
        SchemaSerializer serializer = new SchemaSerializer();
        return serializer.writeEdgeLabel(edgeLabel);
    }

    @Override
    public EdgeLabel readEdgeLabel(HugeGraph graph, BackendEntry backendEntry) {
        if (backendEntry == null) {
            return null;
        }
        BinaryBackendEntry entry = this.convertEntry(backendEntry);

        SchemaSerializer serializer = new SchemaSerializer();
        return serializer.readEdgeLabel(graph, entry);
    }

    @Override
    public BackendEntry writePropertyKey(PropertyKey propertyKey) {
        SchemaSerializer serializer = new SchemaSerializer();
        return serializer.writePropertyKey(propertyKey);
    }

    @Override
    public PropertyKey readPropertyKey(HugeGraph graph,
                                       BackendEntry backendEntry) {
        if (backendEntry == null) {
            return null;
        }
        BinaryBackendEntry entry = this.convertEntry(backendEntry);

        SchemaSerializer serializer = new SchemaSerializer();
        return serializer.readPropertyKey(graph, entry);
    }

    @Override
    public BackendEntry writeIndexLabel(IndexLabel indexLabel) {
        SchemaSerializer serializer = new SchemaSerializer();
        return serializer.writeIndexLabel(indexLabel);
    }

    @Override
    public IndexLabel readIndexLabel(HugeGraph graph,
                                     BackendEntry backendEntry) {
        if (backendEntry == null) {
            return null;
        }
        BinaryBackendEntry entry = this.convertEntry(backendEntry);

        SchemaSerializer serializer = new SchemaSerializer();
        return serializer.readIndexLabel(graph, entry);
    }

    private final class SchemaSerializer {

        private BinaryBackendEntry entry;

        public BinaryBackendEntry writeVertexLabel(VertexLabel schema) {
            this.entry = newBackendEntry(schema);
            writeString(HugeKeys.NAME, schema.name());
            writeEnum(HugeKeys.ID_STRATEGY, schema.idStrategy());
            writeIds(HugeKeys.PROPERTIES, schema.properties());
            writeIds(HugeKeys.PRIMARY_KEYS, schema.primaryKeys());
            writeIds(HugeKeys.NULLABLE_KEYS, schema.nullableKeys());
            writeIds(HugeKeys.INDEX_LABELS, schema.indexLabels());
            writeBool(HugeKeys.ENABLE_LABEL_INDEX, schema.enableLabelIndex());
            writeEnum(HugeKeys.STATUS, schema.status());
            writeUserdata(schema);
            return this.entry;
        }

        public VertexLabel readVertexLabel(HugeGraph graph,
                                           BinaryBackendEntry entry) {
            E.checkNotNull(entry, "entry");
            this.entry = entry;
            Id id = entry.id().origin();
            String name = readString(HugeKeys.NAME);

            VertexLabel vertexLabel = new VertexLabel(graph, id, name);
            vertexLabel.idStrategy(readEnum(HugeKeys.ID_STRATEGY,
                                            IdStrategy.class));
            vertexLabel.properties(readIds(HugeKeys.PROPERTIES));
            vertexLabel.primaryKeys(readIds(HugeKeys.PRIMARY_KEYS));
            vertexLabel.nullableKeys(readIds(HugeKeys.NULLABLE_KEYS));
            vertexLabel.indexLabels(readIds(HugeKeys.INDEX_LABELS));
            vertexLabel.enableLabelIndex(readBool(HugeKeys.ENABLE_LABEL_INDEX));
            vertexLabel.status(readEnum(HugeKeys.STATUS, SchemaStatus.class));
            readUserdata(vertexLabel);
            return vertexLabel;
        }

        public BinaryBackendEntry writeEdgeLabel(EdgeLabel schema) {
            this.entry = newBackendEntry(schema);
            writeString(HugeKeys.NAME, schema.name());
            writeId(HugeKeys.SOURCE_LABEL, schema.sourceLabel());
            writeId(HugeKeys.TARGET_LABEL, schema.targetLabel());
            writeEnum(HugeKeys.FREQUENCY, schema.frequency());
            writeIds(HugeKeys.PROPERTIES, schema.properties());
            writeIds(HugeKeys.SORT_KEYS, schema.sortKeys());
            writeIds(HugeKeys.NULLABLE_KEYS, schema.nullableKeys());
            writeIds(HugeKeys.INDEX_LABELS, schema.indexLabels());
            writeBool(HugeKeys.ENABLE_LABEL_INDEX, schema.enableLabelIndex());
            writeEnum(HugeKeys.STATUS, schema.status());
            writeUserdata(schema);
            return this.entry;
        }

        public EdgeLabel readEdgeLabel(HugeGraph graph,
                                       BinaryBackendEntry entry) {
            E.checkNotNull(entry, "entry");
            this.entry = entry;
            Id id = entry.id().origin();
            String name = readString(HugeKeys.NAME);

            EdgeLabel edgeLabel = new EdgeLabel(graph, id, name);
            edgeLabel.sourceLabel(readId(HugeKeys.SOURCE_LABEL));
            edgeLabel.targetLabel(readId(HugeKeys.TARGET_LABEL));
            edgeLabel.frequency(readEnum(HugeKeys.FREQUENCY, Frequency.class));
            edgeLabel.properties(readIds(HugeKeys.PROPERTIES));
            edgeLabel.sortKeys(readIds(HugeKeys.SORT_KEYS));
            edgeLabel.nullableKeys(readIds(HugeKeys.NULLABLE_KEYS));
            edgeLabel.indexLabels(readIds(HugeKeys.INDEX_LABELS));
            edgeLabel.enableLabelIndex(readBool(HugeKeys.ENABLE_LABEL_INDEX));
            edgeLabel.status(readEnum(HugeKeys.STATUS, SchemaStatus.class));
            readUserdata(edgeLabel);
            return edgeLabel;
        }

        public BinaryBackendEntry writePropertyKey(PropertyKey schema) {
            this.entry = newBackendEntry(schema);
            writeString(HugeKeys.NAME, schema.name());
            writeEnum(HugeKeys.DATA_TYPE, schema.dataType());
            writeEnum(HugeKeys.CARDINALITY, schema.cardinality());
            writeIds(HugeKeys.PROPERTIES, schema.properties());
            writeEnum(HugeKeys.STATUS, schema.status());
            writeUserdata(schema);
            return this.entry;
        }

        public PropertyKey readPropertyKey(HugeGraph graph,
                                           BinaryBackendEntry entry) {
            E.checkNotNull(entry, "entry");
            this.entry = entry;
            Id id = entry.id().origin();
            String name = readString(HugeKeys.NAME);

            PropertyKey propertyKey = new PropertyKey(graph, id, name);
            propertyKey.dataType(readEnum(HugeKeys.DATA_TYPE, DataType.class));
            propertyKey.cardinality(readEnum(HugeKeys.CARDINALITY,
                                             Cardinality.class));
            propertyKey.properties(readIds(HugeKeys.PROPERTIES));
            propertyKey.status(readEnum(HugeKeys.STATUS, SchemaStatus.class));
            readUserdata(propertyKey);
            return propertyKey;
        }

        public BinaryBackendEntry writeIndexLabel(IndexLabel schema) {
            this.entry = newBackendEntry(schema);
            writeString(HugeKeys.NAME, schema.name());
            writeEnum(HugeKeys.BASE_TYPE, schema.baseType());
            writeId(HugeKeys.BASE_VALUE, schema.baseValue());
            writeEnum(HugeKeys.INDEX_TYPE, schema.indexType());
            writeIds(HugeKeys.FIELDS, schema.indexFields());
            writeEnum(HugeKeys.STATUS, schema.status());
            return this.entry;
        }

        public IndexLabel readIndexLabel(HugeGraph graph,
                                         BinaryBackendEntry entry) {
            E.checkNotNull(entry, "entry");
            this.entry = entry;
            Id id = entry.id().origin();
            String name = readString(HugeKeys.NAME);

            IndexLabel indexLabel = new IndexLabel(graph, id, name);
            indexLabel.baseType(readEnum(HugeKeys.BASE_TYPE, HugeType.class));
            indexLabel.baseValue(readId(HugeKeys.BASE_VALUE));
            indexLabel.indexType(readEnum(HugeKeys.INDEX_TYPE,
                                          IndexType.class));
            indexLabel.indexFields(readIds(HugeKeys.FIELDS));
            indexLabel.status(readEnum(HugeKeys.STATUS, SchemaStatus.class));
            return indexLabel;
        }

        private void writeUserdata(SchemaElement schema) {
            String userdataStr = JsonUtil.toJson(schema.userdata());
            writeString(HugeKeys.USER_DATA, userdataStr);
        }

        private void readUserdata(SchemaElement schema) {
            // Parse all user data of a schema element
            byte[] userdataBytes = column(HugeKeys.USER_DATA);
            String userdataStr = StringEncoding.decode(userdataBytes);
            @SuppressWarnings("unchecked")
            Map<String, Object> userdata = JsonUtil.fromJson(userdataStr,
                                                             Map.class);
            for (Map.Entry<String, Object> e : userdata.entrySet()) {
                schema.userdata(e.getKey(), e.getValue());
            }
        }

        private void writeString(HugeKeys key, String value) {
            this.entry.column(formatColumnName(key),
                              StringEncoding.encode(value));
        }

        private String readString(HugeKeys key) {
            return StringEncoding.decode(column(key));
        }

        private void writeEnum(HugeKeys key, SerialEnum value) {
            this.entry.column(formatColumnName(key), new byte[]{value.code()});
        }

        private <T extends SerialEnum> T readEnum(HugeKeys key,
                                                  Class<T> clazz) {
            byte[] value = column(key);
            E.checkState(value.length == 1,
                         "The length of column '%s' must be 1, but is '%s'",
                         key, value.length);
            return SerialEnum.fromCode(clazz, value[0]);
        }

        private void writeId(HugeKeys key, Id value) {
            this.entry.column(formatColumnName(key), writeId(value));
        }

        private Id readId(HugeKeys key) {
            return readId(column(key));
        }

        private void writeIds(HugeKeys key, Collection<Id> value) {
            this.entry.column(formatColumnName(key), writeIds(value));
        }

        private Id[] readIds(HugeKeys key) {
            return readIds(column(key));
        }

        private void writeBool(HugeKeys key, boolean value) {
            this.entry.column(formatColumnName(key),
                              new byte[]{(byte) (value ? 1 : 0)});
        }

        private boolean readBool(HugeKeys key) {
            byte[] value = column(key);
            E.checkState(value.length == 1,
                         "The length of column '%s' must be 1, but is '%s'",
                         key, value.length);
            return value[0] != (byte) 0;
        }

        private byte[] writeId(Id id) {
            int size = 1 + id.length();
            BytesBuffer buffer = BytesBuffer.allocate(size);
            buffer.writeId(id);
            return buffer.bytes();
        }

        private Id readId(byte[] value) {
            BytesBuffer buffer = BytesBuffer.wrap(value);
            return buffer.readId();
        }

        private byte[] writeIds(Collection<Id> ids) {
            E.checkState(ids.size() <= BytesBuffer.UINT16_MAX,
                         "The number of properties of vertex/edge label " +
                         "cannot exceed '%s'", BytesBuffer.UINT16_MAX);
            int size = 2;
            for (Id id : ids) {
                size += (1 + id.length());
            }
            BytesBuffer buffer = BytesBuffer.allocate(size);
            buffer.writeUInt16(ids.size());
            for (Id id : ids) {
                buffer.writeId(id);
            }
            return buffer.bytes();
        }

        private Id[] readIds(byte[] value) {
            BytesBuffer buffer = BytesBuffer.wrap(value);
            int size = buffer.readUInt16();
            Id[] ids = new Id[size];
            for (int i = 0; i < size; i++) {
                Id id = buffer.readId();
                ids[i] = id;
            }
            return ids;
        }

        private byte[] column(HugeKeys key) {
            BackendColumn column = this.entry.column(formatColumnName(key));
            E.checkState(column != null, "Not found key '%s' from entry %s",
                         key, this.entry);
            E.checkNotNull(column.value, "column.value");
            return column.value;
        }

        private byte[] formatColumnName(HugeKeys key) {
            Id id = this.entry.id().origin();
            int size = 1 + id.length() + 1;
            BytesBuffer buffer = BytesBuffer.allocate(size);
            buffer.writeId(id);
            buffer.write(key.code());
            return buffer.bytes();
        }
    }
}
