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

package com.baidu.hugegraph.schema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.tinkerpop.gremlin.structure.Graph;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.schema.builder.SchemaBuilder;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.IdStrategy;
import com.google.common.base.Objects;

public class VertexLabel extends SchemaLabel {

    public static final VertexLabel NONE = new VertexLabel(null, NONE_ID, UNDEF);

    // OLAP_VL means all of vertex labels
    public static final VertexLabel OLAP_VL = new VertexLabel(null, OLAP_ID,
                                                              SchemaElement.OLAP);

    private IdStrategy idStrategy;
    private List<Id> primaryKeys;

    public VertexLabel(final HugeGraph graph, Id id, String name) {
        super(graph, id, name);
        this.idStrategy = IdStrategy.DEFAULT;
        this.primaryKeys = new ArrayList<>();
    }

    @Override
    public HugeType type() {
        return HugeType.VERTEX_LABEL;
    }

    public IdStrategy idStrategy() {
        return this.idStrategy;
    }

    public void idStrategy(IdStrategy idStrategy) {
        this.idStrategy = idStrategy;
    }

    public List<Id> primaryKeys() {
        return Collections.unmodifiableList(this.primaryKeys);
    }

    public void primaryKey(Id id) {
        this.primaryKeys.add(id);
    }

    public void primaryKeys(Id... ids) {
        this.primaryKeys.addAll(Arrays.asList(ids));
    }

    public boolean existsLinkLabel() {
        return this.graph().existsLinkLabel(this.id());
    }

    public boolean hasSameContent(VertexLabel other) {
        return super.hasSameContent(other) &&
               this.idStrategy == other.idStrategy &&
               Objects.equal(this.graph.mapPkId2Name(this.primaryKeys),
                             other.graph.mapPkId2Name(other.primaryKeys));
    }

    public static VertexLabel undefined(HugeGraph graph) {
        return new VertexLabel(graph, NONE_ID, UNDEF);
    }

    public static VertexLabel undefined(HugeGraph graph, Id id) {
        return new VertexLabel(graph, id, UNDEF);
    }

    public String convert2Groovy() {
        StringBuilder builder = new StringBuilder(SCHEMA_PREFIX);
        // Name
        builder.append("vertexLabel").append("(\"")
               .append(this.name())
               .append("\")");

        // Properties
        Set<Id> properties = this.properties();
        if (!properties.isEmpty()) {
            builder.append(".").append("properties(");

            int size = properties.size();
            for (Id id : this.properties()) {
                PropertyKey pk = this.graph.propertyKey(id);
                builder.append("\"")
                       .append(pk.name())
                       .append("\"");
                if (--size > 0) {
                    builder.append(",");
                }
            }
            builder.append(")");
        }

        // Id strategy
        switch (this.idStrategy()) {
            case PRIMARY_KEY:
                builder.append(".primaryKeys(");
                List<Id> pks = this.primaryKeys();
                int size = pks.size();
                for (Id id : pks) {
                    PropertyKey pk = this.graph.propertyKey(id);
                    builder.append("\"")
                           .append(pk.name())
                           .append("\"");
                    if (--size > 0) {
                        builder.append(",");
                    }
                }
                builder.append(")");
                break;
            case CUSTOMIZE_STRING:
                builder.append("useCustomizeStringId()");
                break;
            case CUSTOMIZE_NUMBER:
                builder.append("useCustomizeNumberId()");
                break;
            case CUSTOMIZE_UUID:
                builder.append("useCustomizeUuidId()");
                break;
            case AUTOMATIC:
                builder.append("useAutomaticId()");
                break;
            default:
                throw new AssertionError(String.format(
                          "Invalid id strategy '%s'", this.idStrategy()));
        }

        // Nullable keys
        properties = this.nullableKeys();
        if (!properties.isEmpty()) {
            builder.append(".").append("nullableKeys(");
            int size = properties.size();
            for (Id id : properties) {
                PropertyKey pk = this.graph.propertyKey(id);
                builder.append("\"")
                       .append(pk.name())
                       .append("\"");
                if (--size > 0) {
                    builder.append(",");
                }
            }
            builder.append(")");
        }

        // TTL
        if (this.ttl() != 0) {
            builder.append(".ttl(")
                   .append(this.ttl())
                   .append(")");
            if (this.ttlStartTime() != null) {
                PropertyKey pk = this.graph.propertyKey(this.ttlStartTime());
                builder.append(".ttlStartTime(\"")
                       .append(pk.name())
                       .append("\")");
            }
        }

        // Enable label index
        if (this.enableLabelIndex()) {
            builder.append(".enableLabelIndex(true)");
        }

        // User data
        Map<String, Object> userdata = this.userdata();
        if (userdata.isEmpty()) {
            return builder.toString();
        }
        for (Map.Entry<String, Object> entry : userdata.entrySet()) {
            if (Graph.Hidden.isHidden(entry.getKey())) {
                continue;
            }
            builder.append(".userdata(\"")
                   .append(entry.getKey())
                   .append("\",")
                   .append(entry.getValue())
                   .append(")");
        }

        builder.append(".ifNotExist().create();");
        return builder.toString();
    }

    public interface Builder extends SchemaBuilder<VertexLabel> {

        Id rebuildIndex();

        Builder idStrategy(IdStrategy idStrategy);

        Builder useAutomaticId();

        Builder usePrimaryKeyId();

        Builder useCustomizeStringId();

        Builder useCustomizeNumberId();

        Builder useCustomizeUuidId();

        Builder properties(String... properties);

        Builder primaryKeys(String... keys);

        Builder nullableKeys(String... keys);

        Builder ttl(long ttl);

        Builder ttlStartTime(String ttlStartTime);

        Builder enableLabelIndex(boolean enable);

        Builder userdata(String key, Object value);

        Builder userdata(Map<String, Object> userdata);
    }
}
