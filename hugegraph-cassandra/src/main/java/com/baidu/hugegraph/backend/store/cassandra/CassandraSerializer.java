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

package com.baidu.hugegraph.backend.store.cassandra;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.backend.id.IdUtil;
import com.baidu.hugegraph.backend.serializer.BytesBuffer;
import com.baidu.hugegraph.backend.serializer.TableBackendEntry;
import com.baidu.hugegraph.backend.serializer.TableSerializer;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.schema.PropertyKey;
import com.baidu.hugegraph.schema.SchemaElement;
import com.baidu.hugegraph.structure.HugeElement;
import com.baidu.hugegraph.structure.HugeIndex;
import com.baidu.hugegraph.structure.HugeProperty;
import com.baidu.hugegraph.structure.HugeVertex;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.DataType;
import com.baidu.hugegraph.type.define.HugeKeys;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.InsertionOrderUtil;
import com.baidu.hugegraph.util.JsonUtil;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

public class CassandraSerializer extends TableSerializer {

    @Override
    public CassandraBackendEntry newBackendEntry(HugeType type, Id id) {
        return new CassandraBackendEntry(type, id);
    }

    @Override
    protected TableBackendEntry newBackendEntry(TableBackendEntry.Row row) {
        return new CassandraBackendEntry(row);
    }

    @Override
    protected TableBackendEntry newBackendEntry(HugeIndex index) {
        TableBackendEntry backendEntry = newBackendEntry(index.type(),
                                                         index.id());
        if (index.indexLabel().olap()) {
            backendEntry.olap(true);
        }
        return backendEntry;
    }

    @Override
    protected CassandraBackendEntry convertEntry(BackendEntry backendEntry) {
        if (!(backendEntry instanceof CassandraBackendEntry)) {
            throw new BackendException("Not supported by CassandraSerializer");
        }
        return (CassandraBackendEntry) backendEntry;
    }

    @Override
    protected Set<Object> parseIndexElemIds(TableBackendEntry entry) {
        return ImmutableSet.of(entry.column(HugeKeys.ELEMENT_IDS));
    }

    @Override
    protected Id toId(Number number) {
        return IdGenerator.of(number.longValue());
    }

    @Override
    protected Id[] toIdArray(Object object) {
        assert object instanceof Collection;
        @SuppressWarnings("unchecked")
        Collection<Number> numbers = (Collection<Number>) object;
        Id[] ids = new Id[numbers.size()];
        int i = 0;
        for (Number number : numbers) {
            ids[i++] = toId(number);
        }
        return ids;
    }

    @Override
    protected Object toLongSet(Collection<Id> ids) {
        Set<Long> results = InsertionOrderUtil.newSet();
        for (Id id : ids) {
            results.add(id.asLong());
        }
        return results;
    }

    @Override
    protected Object toLongList(Collection<Id> ids) {
        List<Long> results = new ArrayList<>(ids.size());
        for (Id id : ids) {
            results.add(id.asLong());
        }
        return results;
    }

    @Override
    protected void formatProperties(HugeElement element,
                                    TableBackendEntry.Row row) {
        if (!element.hasProperties() && !element.removed()) {
            row.column(HugeKeys.PROPERTIES, ImmutableMap.of());
        } else {
            // Format properties
            for (HugeProperty<?> prop : element.getProperties()) {
                this.formatProperty(prop, row);
            }
        }
    }

    @Override
    protected void parseProperties(HugeElement element,
                                   TableBackendEntry.Row row) {
        Map<Number, Object> props = row.column(HugeKeys.PROPERTIES);
        for (Map.Entry<Number, Object> prop : props.entrySet()) {
            Id pkeyId = this.toId(prop.getKey());
            this.parseProperty(pkeyId, prop.getValue(), element);
        }
    }

    @Override
    public BackendEntry writeOlapVertex(HugeVertex vertex) {
        CassandraBackendEntry entry = newBackendEntry(HugeType.OLAP,
                                                      vertex.id());
        entry.column(HugeKeys.ID, this.writeId(vertex.id()));

        Collection<HugeProperty<?>> properties = vertex.getProperties();
        E.checkArgument(properties.size() == 1,
                        "Expect only 1 property for olap vertex, but got %s",
                        properties.size());
        HugeProperty<?> property = properties.iterator().next();
        PropertyKey pk = property.propertyKey();
        entry.subId(pk.id());
        entry.column(HugeKeys.PROPERTY_VALUE,
                     this.writeProperty(pk, property.value()));
        entry.olap(true);
        return entry;
    }

    @Override
    protected Object writeProperty(PropertyKey propertyKey, Object value) {
        BytesBuffer buffer = BytesBuffer.allocate(BytesBuffer.BUF_PROPERTY);
        if (propertyKey == null) {
            /*
             * Since we can't know the type of the property value in some
             * scenarios so need to construct a fake property key to
             * serialize to reuse code.
             */
            propertyKey = new PropertyKey(null, IdGenerator.of(0L), "fake");
            propertyKey.dataType(DataType.fromClass(value.getClass()));
        }
        buffer.writeProperty(propertyKey, value);
        buffer.forReadWritten();
        return buffer.asByteBuffer();
    }

    @Override
    @SuppressWarnings("unchecked")
    protected <T> T readProperty(PropertyKey pkey, Object value) {
        BytesBuffer buffer = BytesBuffer.wrap((ByteBuffer) value);
        return (T) buffer.readProperty(pkey);
    }

    @Override
    protected Object writeId(Id id) {
        return IdUtil.writeBinString(id);
    }

    @Override
    protected Id readId(Object id) {
        return IdUtil.readBinString(id);
    }

    @Override
    protected void writeUserdata(SchemaElement schema,
                                 TableBackendEntry entry) {
        assert entry instanceof CassandraBackendEntry;
        for (Map.Entry<String, Object> e : schema.userdata().entrySet()) {
            entry.column(HugeKeys.USER_DATA, e.getKey(),
                         JsonUtil.toJson(e.getValue()));
        }
    }

    @Override
    protected void readUserdata(SchemaElement schema,
                                TableBackendEntry entry) {
        assert entry instanceof CassandraBackendEntry;
        // Parse all user data of a schema element
        Map<String, String> userdata = entry.column(HugeKeys.USER_DATA);
        for (Map.Entry<String, String> e : userdata.entrySet()) {
            String key = e.getKey();
            Object value = JsonUtil.fromJson(e.getValue(), Object.class);
            schema.userdata(key, value);
        }
    }
}
