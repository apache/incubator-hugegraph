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
import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.backend.id.SplicingIdGenerator;
import com.baidu.hugegraph.backend.serializer.BytesBuffer;
import com.baidu.hugegraph.exception.NotSupportException;
import com.baidu.hugegraph.schema.IndexLabel;
import com.baidu.hugegraph.schema.SchemaElement;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.DataType;
import com.baidu.hugegraph.type.define.IndexType;
import com.baidu.hugegraph.util.E;
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
        IndexType indexType = this.indexLabel.indexType();
        if (indexType == IndexType.SECONDARY) {
            return HugeType.SECONDARY_INDEX;
        } else if (indexType == IndexType.RANGE) {
            return HugeType.RANGE_INDEX;
        } else {
            assert indexType == IndexType.SEARCH;
            throw new NotSupportException("index '%s' currently", indexType);
        }
    }

    public Id id() {
        return formatIndexId(type(), this.indexLabel(), this.fieldValues());
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

    public static HugeIndex parseIndexId(HugeGraph graph,
                                         HugeType type, byte[] id) {
        Object values;
        IndexLabel indexLabel;

        if (type == HugeType.SECONDARY_INDEX) {
            Id idObject = IdGenerator.of(id, false);
            String[] parts = SplicingIdGenerator.parse(idObject);
            E.checkState(parts.length == 2, "Invalid secondary index id");
            values = parts[0];
            Id label = SchemaElement.schemaId(parts[1]);
            indexLabel = IndexLabel.label(graph, label);
        } else {
            assert type == HugeType.RANGE_INDEX;
            final int labelLength = 4;
            E.checkState(id.length > labelLength, "Invalid range index id");
            BytesBuffer buffer = BytesBuffer.wrap(id);
            Id label = IdGenerator.of(buffer.readInt());
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
        }
        HugeIndex index = new HugeIndex(indexLabel);
        index.fieldValues(values);
        return index;
    }

    public static Id formatIndexId(HugeType type, Id indexLabel,
                                   Object fieldValues) {
        if (type == HugeType.SECONDARY_INDEX) {
            String value = fieldValues == null ? "?" : fieldValues.toString();
            return SplicingIdGenerator.splicing(value, indexLabel.asString());
        } else {
            assert type == HugeType.RANGE_INDEX;
            BytesBuffer buffer = BytesBuffer.allocate(16);
            buffer.writeInt((int) indexLabel.asLong());
            if (fieldValues != null) {
                E.checkState(fieldValues instanceof Number,
                             "Field value of range index must be number: %s",
                             fieldValues.getClass().getSimpleName());
                byte[] value = number2bytes((Number) fieldValues);
                buffer.write(value);
            }
            return buffer.asId();
        }
    }

    public static byte[] number2bytes(Number number) {
        return NumericUtil.numberToSortableBytes(number);
    }

    public static Number bytes2number(byte[] bytes, Class<?> clazz) {
        return NumericUtil.sortableBytesToNumber(bytes, clazz);
    }
}
