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

package org.apache.hugegraph.serializer;

import static org.apache.hugegraph.schema.SchemaElement.UNDEF;

import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.Map;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.NotImplementedException;
import org.apache.hugegraph.util.Bytes;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

import org.apache.hugegraph.HugeGraphSupplier;
import org.apache.hugegraph.backend.BackendColumn;
import org.apache.hugegraph.backend.BinaryId;
import org.apache.hugegraph.exception.HugeException;
import org.apache.hugegraph.id.EdgeId;
import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.id.IdGenerator;
import org.apache.hugegraph.schema.EdgeLabel;
import org.apache.hugegraph.schema.PropertyKey;
import org.apache.hugegraph.schema.SchemaElement;
import org.apache.hugegraph.schema.VertexLabel;
import org.apache.hugegraph.structure.BaseEdge;
import org.apache.hugegraph.structure.BaseElement;
import org.apache.hugegraph.structure.BaseProperty;
import org.apache.hugegraph.structure.BaseVertex;
import org.apache.hugegraph.structure.Index;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.type.define.Cardinality;
import org.apache.hugegraph.type.define.EdgeLabelType;
import org.apache.hugegraph.util.StringEncoding;
import com.google.common.primitives.Longs;

public class BinaryElementSerializer {
    static final BinaryElementSerializer INSTANCE =
            new BinaryElementSerializer();
    static Logger log = Log.logger(BinaryElementSerializer.class);

    public static BinaryElementSerializer getInstance() {
        return INSTANCE;
    }

    protected void parseProperty(HugeGraphSupplier graph, Id pkeyId,
                                 BytesBuffer buffer,
                                 BaseElement owner) {
        PropertyKey pkey = graph != null ?
                           graph.propertyKey(pkeyId) :
                           new PropertyKey(graph, pkeyId, "");
        // Parse value
        Object value = buffer.readProperty(pkey);
        // Set properties of vertex/edge
        if (pkey.cardinality() == Cardinality.SINGLE) {
            owner.addProperty(pkey, value);
        } else {
            if (!(value instanceof Collection)) {
                throw new HugeException(
                        "Invalid value of non-single property: %s", value);
            }
            owner.addProperty(pkey, value);
        }
    }

    public void parseProperties(HugeGraphSupplier graph, BytesBuffer buffer,
                                BaseElement owner) {
        int size = buffer.readVInt();
        assert size >= 0;
        for (int i = 0; i < size; i++) {
            Id pkeyId = IdGenerator.of(buffer.readVInt());
            this.parseProperty(graph, pkeyId, buffer, owner);
        }
    }

    /**
     * 将顶点 kv 数据反序列成 BaseVertex 类型顶点
     *
     * @param vertexCol    必须是顶点数据的 column
     * @param vertex vertex==null 时，用于算子下沉，将 col 数据反序列成 BaseVertex ;
     *               vertex!=null 时，将 col 信息增加到 vertex 中
     */
    public BaseVertex parseVertex(HugeGraphSupplier graph, BackendColumn vertexCol,
                                  BaseVertex vertex) {
        if (vertex == null) {
            BinaryId binaryId =
                    BytesBuffer.wrap(vertexCol.name).parseId(HugeType.VERTEX);
            vertex = new BaseVertex(binaryId.origin(), VertexLabel.NONE);
        }

        if (ArrayUtils.isEmpty(vertexCol.value)) {
            // 不需要解析 vertex 的 properties
            return vertex;
        }
        BytesBuffer buffer = BytesBuffer.wrap(vertexCol.value);
        Id labelId = buffer.readId();
        // Parse vertex label
        if (graph != null) {
            VertexLabel label = graph.vertexLabelOrNone(labelId);
            vertex.correctVertexLabel(label);
        } else {
            VertexLabel label = new VertexLabel(null, labelId, UNDEF);
            vertex.correctVertexLabel(label);
        }
        // Parse properties
        this.parseProperties(graph, buffer, vertex);

        // Parse vertex expired time if needed
        if (buffer.remaining() > 0 /*edge.hasTtl()*/) {
            this.parseExpiredTime(buffer, vertex);
        }
        return vertex;
    }

    /**
     * 将顶点 kv 数据反序列成 BaseVertex 类型顶点
     *
     * @param olapVertexCol    必须是顶点数据的 column
     * @param vertex vertex==null 时，用于算子下沉，将 col 数据反序列成 olapBaseVertex ;
     *               vertex!=null 时，将 col 信息增加到 olapBaseVertex 中
     */
    public BaseVertex parseVertexOlap(HugeGraphSupplier graph,
                                      BackendColumn olapVertexCol, BaseVertex vertex) {
        if (vertex == null) {
            BytesBuffer idBuffer = BytesBuffer.wrap(olapVertexCol.name);
            // read olap property id
            idBuffer.readId();
            // read vertex id which olap property belongs to
            Id vertexId = idBuffer.readId();
            vertex = new BaseVertex(vertexId, VertexLabel.NONE);
        }

        BytesBuffer buffer = BytesBuffer.wrap(olapVertexCol.value);
        Id pkeyId = IdGenerator.of(buffer.readVInt());
        this.parseProperty(graph, pkeyId, buffer, vertex);
        return vertex;
    }

    /**
     * @param cols 反序列化出一个完整的顶点有可能需要多个 cols,
     *             第一个 col 是代表 g+v 表中的普通顶点信息，之后每个 col 代表 olap 表中存储的 olap 顶点
     */
    public BaseVertex parseVertexFromCols(HugeGraphSupplier graph,
                                          BackendColumn... cols) {
        assert cols.length > 0;
        BaseVertex vertex = null;
        for (int index = 0; index < cols.length; index++) {
            BackendColumn col = cols[index];
            if (index == 0) {
                vertex = this.parseVertex(graph, col, vertex);
            } else {
                this.parseVertexOlap(graph, col, vertex);
            }
        }
        return vertex;
    }

    public BaseEdge parseEdge(HugeGraphSupplier graph, BackendColumn edgeCol,
                              BaseVertex ownerVertex,
                              boolean withEdgeProperties) {
        // owner-vertex + dir + edge-label.id() + subLabel.id() +
        // + sort-values + other-vertex

        BytesBuffer buffer = BytesBuffer.wrap(edgeCol.name);
        // Consume owner-vertex id
        Id id = buffer.readId();
        if (ownerVertex == null) {
            ownerVertex = new BaseVertex(id, VertexLabel.NONE);
        }

        E.checkState(buffer.remaining() > 0, "Missing column type");

        byte type = buffer.read();
        if (type == HugeType.EDGE_IN.code() ||
            type == HugeType.EDGE_OUT.code()) {
            E.checkState(true,
                         "Invalid column(%s) with unknown type(%s): 0x%s",
                         id, type & 0xff, Bytes.toHex(edgeCol.name));
        }

        Id labelId = buffer.readId();
        Id subLabelId = buffer.readId();
        String sortValues = buffer.readStringWithEnding();
        Id otherVertexId = buffer.readId();
        boolean direction = EdgeId.isOutDirectionFromCode(type);
        BaseEdge edge;
        EdgeLabel edgeLabel;
        if (graph == null) { /* when calculation sinking */
            edgeLabel = new EdgeLabel(null, subLabelId, UNDEF);
            // 如果这里不相等，需要加上 fatherId，以便正确的算子下沉
            if (subLabelId != labelId) {
                edgeLabel.edgeLabelType(EdgeLabelType.SUB);
                edgeLabel.fatherId(labelId);
            }

        } else {
            edgeLabel = graph.edgeLabelOrNone(subLabelId);
        }
        edge = BaseEdge.constructEdge(graph, ownerVertex, direction,
                                      edgeLabel, sortValues, otherVertexId);

        if (!withEdgeProperties /*&& !edge.hasTtl()*/) {
            // only skip properties for edge without ttl
            // todo: save expiredTime before properties
            return edge;
        }

        if (ArrayUtils.isEmpty(edgeCol.value)) {
            // There is no edge-properties here.
            return edge;
        }

        // Parse edge-id + edge-properties
        buffer = BytesBuffer.wrap(edgeCol.value);

        // Parse edge properties
        this.parseProperties(graph, buffer, edge);

        /* 先跳过 ttl 解析过程
         *  通过 edge 判断不出有没有 ttl 了，需要通过 bytebuffer 长度判断了 */
//        // Parse edge expired time if needed
        if (buffer.remaining() > 0 /*edge.hasTtl()*/) {
            this.parseExpiredTime(buffer, edge);
        }
        return edge;
    }

    /**
     * @param graph 解析索引的时候 graph 不能为 null
     * @param index 为 null 时，算子下沉使用，store 可以根据一条 col 数据还原 index
     */
    public Index parseIndex(HugeGraphSupplier graph, BackendColumn indexCol,
                            Index index) {
        HugeType indexType = parseIndexType(indexCol);

        BytesBuffer buffer = BytesBuffer.wrap(indexCol.name);
        BinaryId indexId = buffer.readIndexId(indexType);
        Id elemId = buffer.readId();

        if (index == null) {
            index = Index.parseIndexId(graph, indexType, indexId.asBytes());
        }

        long expiredTime = 0L;

        if (indexCol.value.length > 0) {

            // 获取分隔符地址
            int delimiterIndex =
                    Bytes.indexOf(indexCol.value, BytesBuffer.STRING_ENDING_BYTE);

            if (delimiterIndex >= 0) {
                // 分隔符在数据中，则需要从数据中解析
                // 1. field value 真是内容
                byte[] fieldValueBytes =
                        Arrays.copyOfRange(indexCol.value, 0, delimiterIndex);
                if (fieldValueBytes.length > 0) {
                    index.fieldValues(StringEncoding.decode(fieldValueBytes));
                }

                // 2. 过期时间
                byte[] expiredTimeBytes =
                        Arrays.copyOfRange(indexCol.value, delimiterIndex + 1,
                                           indexCol.value.length);

                if (expiredTimeBytes.length > 0) {
                    byte[] rawBytes =
                            Base64.getDecoder().decode(expiredTimeBytes);
                    if (rawBytes.length >= Longs.BYTES) {
                        expiredTime = Longs.fromByteArray(rawBytes);
                    }
                }
            } else {
                // 仅有 field value 数据
                index.fieldValues(StringEncoding.decode(indexCol.value));
            }
        }

        index.elementIds(elemId, expiredTime);
        return index;
    }

    public BackendColumn parseIndex(BackendColumn indexCol) {
        // 自解析索引
        throw new NotImplementedException(
                "BinaryElementSerializer.parseIndex");
    }


    public BackendColumn writeVertex(BaseVertex vertex) {
        if (vertex.olap()) {
            return this.writeOlapVertex(vertex);
        }

        BytesBuffer bufferName = BytesBuffer.allocate(vertex.id().length());
        bufferName.writeId(vertex.id());

        int propsCount = vertex.getProperties().size();
        BytesBuffer buffer = BytesBuffer.allocate(8 + 16 * propsCount);

        // Write vertex label
        buffer.writeId(vertex.schemaLabel().id());

        // Write all properties of the vertex
        this.formatProperties(vertex.getProperties().values(), buffer);

        // Write vertex expired time if needed
        if (vertex.hasTtl()) {
            this.formatExpiredTime(vertex.expiredTime(), buffer);
        }

        return BackendColumn.of(bufferName.bytes(), buffer.bytes());
    }

    public BackendColumn writeOlapVertex(BaseVertex vertex) {
        BytesBuffer buffer = BytesBuffer.allocate(8 + 16);

        BaseProperty<?> baseProperty = vertex.getProperties().values()
                                             .iterator().next();
        PropertyKey propertyKey = baseProperty.propertyKey();
        buffer.writeVInt(SchemaElement.schemaId(propertyKey.id()));
        buffer.writeProperty(propertyKey.cardinality(), propertyKey.dataType(),
                             baseProperty.value());

        // olap 表合并，key 为 {property_key_id}{vertex_id}
        BytesBuffer bufferName =
                BytesBuffer.allocate(1 + propertyKey.id().length() + 1 +
                                     vertex.id().length());
        bufferName.writeId(propertyKey.id());
        bufferName.writeId(vertex.id()).bytes();

        return BackendColumn.of(bufferName.bytes(), buffer.bytes());
    }

    public BackendColumn writeEdge(BaseEdge edge) {
        byte[] name = this.formatEdgeName(edge);
        byte[] value = this.formatEdgeValue(edge);
        return BackendColumn.of(name, value);
    }

    /**
     * 将一个索引数据转换为一个 BackendColumn
     */
    public BackendColumn writeIndex(Index index) {
        return BackendColumn.of(formatIndexName(index),
                                formatIndexValue(index));
    }

    private byte[] formatIndexName(Index index) {
        BytesBuffer buffer;
        Id elemId = index.elementId();
        Id indexId = index.id();
        HugeType type = index.type();
        int idLen = 1 + elemId.length() + 1 + indexId.length();
        buffer = BytesBuffer.allocate(idLen);
        // Write index-id
        buffer.writeIndexId(indexId, type);
        // Write element-id
        buffer.writeId(elemId);

        return buffer.bytes();
    }

    /**
     * @param index value
     * @return
     *
     * format
     * | empty(field-value) | 0x00  |  base64(expiredtime) |
     */
    private byte[] formatIndexValue(Index index) {
        if (index.hasTtl()) {
            BytesBuffer valueBuffer = BytesBuffer.allocate(14);

            valueBuffer.write(BytesBuffer.STRING_ENDING_BYTE);
            byte[] ttlBytes =
                    Base64.getEncoder().encode(Longs.toByteArray(index.expiredTime()));
            valueBuffer.write(ttlBytes);

            return valueBuffer.bytes();
        }

        return null;
    }

    public BackendColumn mergeCols(BackendColumn vertexCol, BackendColumn... olapVertexCols) {
        if (olapVertexCols.length == 0) {
            return vertexCol;
        }
        BytesBuffer mergedBuffer = BytesBuffer.allocate(
                vertexCol.value.length + olapVertexCols.length * 16);

        BytesBuffer buffer = BytesBuffer.wrap(vertexCol.value);
        Id vl = buffer.readId();
        int size = buffer.readVInt();

        mergedBuffer.writeId(vl);
        mergedBuffer.writeVInt(size + olapVertexCols.length);
        // 优先写入 vertexCol 的属性，因为 vertexCol 可能包含 ttl
        for (BackendColumn olapVertexCol : olapVertexCols) {
            mergedBuffer.write(olapVertexCol.value);
        }
        mergedBuffer.write(buffer.remainingBytes());

        return BackendColumn.of(vertexCol.name, mergedBuffer.bytes());
    }

    public BaseElement index2Element(HugeGraphSupplier graph,
                                     BackendColumn indexCol) {
        throw new NotImplementedException(
                "BinaryElementSerializer.index2Element");
    }

    public byte[] formatEdgeName(BaseEdge edge) {
        // owner-vertex + dir + edge-label + sort-values + other-vertex
        return BytesBuffer.allocate(BytesBuffer.BUF_EDGE_ID)
                          .writeEdgeId(edge.id()).bytes();
    }


    protected byte[] formatEdgeValue(BaseEdge edge) {
        Map<Id, BaseProperty<?>> properties = edge.getProperties();
        int propsCount = properties.size();
        BytesBuffer buffer = BytesBuffer.allocate(4 + 16 * propsCount);

        // Write edge properties
        this.formatProperties(properties.values(), buffer);

        // Write edge expired time if needed
        if (edge.hasTtl()) {
            this.formatExpiredTime(edge.expiredTime(), buffer);
        }

        return buffer.bytes();
    }

    public void formatProperties(Collection<BaseProperty<?>> props,
                                 BytesBuffer buffer) {
        // Write properties size
        buffer.writeVInt(props.size());

        // Write properties data
        for (BaseProperty<?> property : props) {
            PropertyKey pkey = property.propertyKey();
            buffer.writeVInt(SchemaElement.schemaId(pkey.id()));
            buffer.writeProperty(pkey.cardinality(), pkey.dataType(),
                                 property.value());
        }
    }


    public void formatExpiredTime(long expiredTime, BytesBuffer buffer) {
        buffer.writeVLong(expiredTime);
    }

    protected void parseExpiredTime(BytesBuffer buffer, BaseElement element) {
        element.expiredTime(buffer.readVLong());
    }

    private HugeType parseIndexType(BackendColumn col) {
        /**
         *   参考 formatIndexName 方法
         *   range 类型索引 col.name 第一位写的是 type.code (1 byte)
         *   其他类型索引将会写入头两位写入 type.name (2 byte)
         */
        byte first = col.name[0];
        byte second = col.name[1];
        if (first < 0) {
            return HugeType.fromCode(first);
        }
        assert second >= 0;
        String type = new String(new byte[]{first, second});
        return HugeType.fromString(type);
    }

    /** 计算点/边的ownerid
     * @param element
     * @return
     */
    public static Id ownerId(BaseElement element) {
        if (element instanceof BaseVertex) {
            return element.id();
        } else if (element instanceof BaseEdge) {
            return ((EdgeId) element.id()).ownerVertexId();
        } else {
            throw new IllegalArgumentException("Only support get ownerid" +
                                               " of BaseVertex or BaseEdge");
        }
    }

    /**
     * 计算索引的 ownerid
     * @param index
     * @return
     */
    public static Id ownerId(Index index) {
        Id elementId = index.elementId();

        Id ownerId = null;
        if (elementId instanceof EdgeId) {
            // 边 ID
            EdgeId edgeId = (EdgeId) elementId;
            ownerId = edgeId.ownerVertexId();
        } else {
            // olap 索引
            // 普通 vertex 索引
            // 普通二级索引
            // 点/边 LabelIndex

            ownerId = elementId;
        }

        return ownerId;
    }
}
