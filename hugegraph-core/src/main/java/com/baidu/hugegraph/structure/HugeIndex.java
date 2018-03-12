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

import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.backend.id.SplicingIdGenerator;
import com.baidu.hugegraph.exception.NotSupportException;
import com.baidu.hugegraph.schema.IndexLabel;
import com.baidu.hugegraph.schema.PropertyKey;
import com.baidu.hugegraph.schema.SchemaElement;
import com.baidu.hugegraph.type.HugeType;
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
        return formatIndexId(type(), indexLabel(), fieldValues());
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
                                         HugeType type, Id id) {
        Id label;
        Object values;
        IndexLabel indexLabel;

        if (type == HugeType.SECONDARY_INDEX) {
            String[] parts = SplicingIdGenerator.parse(id);
            E.checkState(parts.length == 2, "Invalid secondary index id");
            values = parts[0];
            label = SchemaElement.schemaId(parts[1]);
            indexLabel = IndexLabel.label(graph, label);
        } else {
            assert type == HugeType.RANGE_INDEX;
            // TODO: parse from bytes id
            String str = id.asString();
            final int offset = 4;
            E.checkState(str.length() > offset, "Invalid range index id");
            label = IdGenerator.of((int) string2number(str.substring(0, offset),
                                                       Integer.class));
            indexLabel = IndexLabel.label(graph, label);
            List<Id> fields = indexLabel.indexFields();
            E.checkState(fields.size() == 1, "Invalid range index fields");
            PropertyKey pk = graph.propertyKey(fields.get(0));
            E.checkState(pk.dataType().isNumber(),
                         "Invalid range index field type");
            values = string2number(str.substring(offset),
                                   pk.dataType().clazz());
        }
        HugeIndex index = new HugeIndex(indexLabel);
        index.fieldValues(values);
        return index;
    }

    public static Id formatIndexId(HugeType type, Id indexLabel,
                                   Object fieldValues) {
        if (type == HugeType.SECONDARY_INDEX) {
            String value = fieldValues == null ? "<?>" : fieldValues.toString();
            return SplicingIdGenerator.splicing(value, indexLabel.asString());
        } else {
            assert type == HugeType.RANGE_INDEX;
            String value = "";
            if (fieldValues != null) {
                E.checkState(fieldValues instanceof Number,
                             "Field value of range index must be number: %s",
                             fieldValues.getClass().getSimpleName());
                value = number2string((Number) fieldValues);
            }
            // TODO: use bytes id
            String index = number2string((int) indexLabel.asLong());
            return IdGenerator.of(index + value);
        }
    }

    public static String number2string(Number number) {
        byte[] bytes = NumericUtil.numberToSortableBytes(number);
        try {
            // TODO: use bytes id
            return new String(bytes, "ISO-8859-1");
        } catch (UnsupportedEncodingException e) {
            throw new BackendException(e);
        }
    }

    public static Number string2number(String value, Class<?> clazz) {
        try {
            byte[] bytes = value.getBytes("ISO-8859-1");
            return NumericUtil.sortableBytesToNumber(bytes, clazz);
        } catch (UnsupportedEncodingException e) {
            throw new BackendException(e);
        }
    }
}
