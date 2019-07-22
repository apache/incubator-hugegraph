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

package com.baidu.hugegraph.structure;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.Id.IdType;
import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.backend.id.SplicingIdGenerator;
import com.baidu.hugegraph.backend.serializer.BytesBuffer;
import com.baidu.hugegraph.schema.IndexLabel;
import com.baidu.hugegraph.schema.SchemaElement;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.DataType;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.HashUtil;
import com.baidu.hugegraph.util.NumericUtil;

public class HugeIndex implements GraphType {

    private Object fieldValues;
    private IndexLabel indexLabel;
    private Set<Id> elementIds;

    public HugeIndex(IndexLabel indexLabel) {
        E.checkNotNull(indexLabel, "label");
        E.checkNotNull(indexLabel.id(), "label id");
        this.indexLabel = indexLabel;
        this.elementIds = new LinkedHashSet<>();
        this.fieldValues = null;
    }

    @Override
    public String name() {
        return this.indexLabel.name();
    }

    @Override
    public HugeType type() {
        return this.indexLabel.indexType().type();
    }

    public Id id() {
        return formatIndexId(type(), this.indexLabel(), this.fieldValues());
    }

    public Id hashId() {
        return formatIndexHashId(type(), this.indexLabel(), this.fieldValues());
    }

    public Object fieldValues() {
        return this.fieldValues;
    }

    public void fieldValues(Object fieldValues) {
        this.fieldValues = fieldValues;
    }

    public Id indexLabel() {
        return this.indexLabel.id();
    }

    public Id elementId() {
        E.checkState(this.elementIds.size() == 1,
                     "Expect one element id, actual %s",
                     this.elementIds.size());
        return this.elementIds.iterator().next();
    }

    public Set<Id> elementIds() {
        return Collections.unmodifiableSet(this.elementIds);
    }

    public void elementIds(Id... elementIds) {
        this.elementIds.addAll(Arrays.asList(elementIds));
    }

    public void resetElementIds() {
        this.elementIds = new LinkedHashSet<>();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof HugeIndex)) {
            return false;
        }

        HugeIndex other = (HugeIndex) obj;
        return this.id().equals(other.id());
    }

    @Override
    public int hashCode() {
        return this.id().hashCode();
    }

    @Override
    public String toString() {
        return String.format("{label=%s<%s>, fieldValues=%s, elementIds=%s}",
                             this.indexLabel.name(),
                             this.indexLabel.indexType().string(),
                             this.fieldValues, this.elementIds);
    }

    public static Id formatIndexHashId(HugeType type, Id indexLabel,
                                       Object fieldValues) {
        E.checkState(!type.isRangeIndex(),
                     "RangeIndex can't return a hash id");
        String value = fieldValues == null ? "" : fieldValues.toString();
        return formatIndexId(type, indexLabel, HashUtil.hash(value));
    }

    public static Id formatIndexId(HugeType type, Id indexLabel,
                                   Object fieldValues) {
        switch (type) {
            case SHARD_INDEX:
            case SEARCH_INDEX:
            case SECONDARY_INDEX:
                String v = fieldValues == null ? "" : fieldValues.toString();
                /*
                 * Modify order between index label and field-values to put the
                 * index label in front(hugegraph-1317)
                 */
                return SplicingIdGenerator.splicing(indexLabel.asString(), v);
            case RANGE4_INDEX:
            case RANGE8_INDEX:
                BytesBuffer buffer = BytesBuffer.allocate(16);
                buffer.writeInt((int) indexLabel.asLong());
                if (fieldValues != null) {
                    E.checkState(fieldValues instanceof Number,
                                 "Field value of range index must be number:" +
                                 " %s", fieldValues.getClass().getSimpleName());
                    byte[] bytes = number2bytes((Number) fieldValues);
                    buffer.write(bytes);
                }
                return buffer.asId();
            default:
                throw new AssertionError("Invalid index type " + type);
        }
    }

    public static HugeIndex parseIndexId(HugeGraph graph, HugeType type,
                                         byte[] id) {
        Object values;
        IndexLabel indexLabel;
        switch (type) {
            case SEARCH_INDEX:
            case SECONDARY_INDEX:
            case SHARD_INDEX:
                Id idObject = IdGenerator.of(id, IdType.STRING);
                String[] parts = SplicingIdGenerator.parse(idObject);
                E.checkState(parts.length == 2, "Invalid secondary index id");
                Id label = SchemaElement.schemaId(parts[0]);
                indexLabel = IndexLabel.label(graph, label);
                values = parts[1];
                break;
            case RANGE4_INDEX:
            case RANGE8_INDEX:
                final int labelLength = 4;
                E.checkState(id.length > labelLength, "Invalid range index id");
                BytesBuffer buffer = BytesBuffer.wrap(id);
                label = IdGenerator.of(buffer.readInt());
                indexLabel = IndexLabel.label(graph, label);
                List<Id> fields = indexLabel.indexFields();
                E.checkState(fields.size() == 1, "Invalid range index fields");
                DataType dataType = graph.propertyKey(fields.get(0)).dataType();
                E.checkState(dataType.isNumber() || dataType.isDate(),
                             "Invalid range index field type");
                Class<?> clazz = dataType.isNumber() ?
                                 dataType.clazz() :
                                 DataType.LONG.clazz();
                values = bytes2number(buffer.read(id.length - labelLength), clazz);
                break;
            default:
                throw new AssertionError("Invalid index type " + type);
        }
        HugeIndex index = new HugeIndex(indexLabel);
        index.fieldValues(values);
        return index;
    }

    public static byte[] number2bytes(Number number) {
        if (number instanceof Byte) {
            // Handle byte as integer to store as 4 bytes in RANGE4_INDEX
            number = number.intValue();
        }
        return NumericUtil.numberToSortableBytes(number);
    }

    public static Number bytes2number(byte[] bytes, Class<?> clazz) {
        return NumericUtil.sortableBytesToNumber(bytes, clazz);
    }
}
