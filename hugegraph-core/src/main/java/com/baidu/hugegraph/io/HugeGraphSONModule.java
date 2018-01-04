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

package com.baidu.hugegraph.io;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.tinkerpop.gremlin.structure.io.graphson.TinkerPopJacksonModule;
import org.apache.tinkerpop.shaded.jackson.core.JsonGenerator;
import org.apache.tinkerpop.shaded.jackson.core.JsonParser;
import org.apache.tinkerpop.shaded.jackson.databind.DeserializationContext;
import org.apache.tinkerpop.shaded.jackson.databind.SerializerProvider;
import org.apache.tinkerpop.shaded.jackson.databind.deser.std.StdDeserializer;
import org.apache.tinkerpop.shaded.jackson.databind.jsontype.TypeSerializer;
import org.apache.tinkerpop.shaded.jackson.databind.ser.std.StdSerializer;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.id.EdgeId;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.backend.serializer.TextBackendEntry;
import com.baidu.hugegraph.backend.serializer.TextSerializer;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.schema.EdgeLabel;
import com.baidu.hugegraph.schema.IndexLabel;
import com.baidu.hugegraph.schema.PropertyKey;
import com.baidu.hugegraph.schema.SchemaElement;
import com.baidu.hugegraph.schema.VertexLabel;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.HugeKeys;
import com.baidu.hugegraph.util.JsonUtil;

@SuppressWarnings("serial")
public class HugeGraphSONModule extends TinkerPopJacksonModule {

    private static final long serialVersionUID = 6480426922914059122L;

    private static final String TYPE_NAMESPACE = "hugegraph";

    private static final Map<HugeGraph, HugeGraphSONModule> INSTANCES;
    @SuppressWarnings("rawtypes")
    private static final Map<Class, String> TYPE_DEFINITIONS;

    private GraphSONSerializer textSerializer;

    static {
        INSTANCES = new ConcurrentHashMap<>();
        TYPE_DEFINITIONS = new ConcurrentHashMap<>();
        TYPE_DEFINITIONS.put(Optional.class, "Optional");
        // HugeGraph releated serializer
        TYPE_DEFINITIONS.put(IdGenerator.StringId.class, "StringId");
        TYPE_DEFINITIONS.put(IdGenerator.LongId.class, "LongId");
        TYPE_DEFINITIONS.put(EdgeId.class, "EdgeId");
        TYPE_DEFINITIONS.put(PropertyKey.class, "PropertyKey");
        TYPE_DEFINITIONS.put(VertexLabel.class, "VertexLabel");
        TYPE_DEFINITIONS.put(EdgeLabel.class, "EdgeLabel");
        TYPE_DEFINITIONS.put(IndexLabel.class, "IndexLabel");
    }

    public static HugeGraphSONModule instance(HugeGraph graph) {
        HugeGraphSONModule instance = INSTANCES.get(graph);
        if (instance == null) {
            instance = new HugeGraphSONModule(graph);
            INSTANCES.putIfAbsent(graph, instance);
        }
        return instance;
    }

    private HugeGraphSONModule(HugeGraph graph) {
        super(TYPE_NAMESPACE);
        this.textSerializer = new GraphSONSerializer(graph);

        addSerializer(IdGenerator.StringId.class,
                      new IdSerializer<>(IdGenerator.StringId.class));
        addDeserializer(IdGenerator.StringId.class,
                        new IdDeserializer<>(IdGenerator.StringId.class));
        addSerializer(IdGenerator.LongId.class,
                      new IdSerializer<>(IdGenerator.LongId.class));
        addDeserializer(IdGenerator.LongId.class,
                        new IdDeserializer<>(IdGenerator.LongId.class));
        addSerializer(EdgeId.class, new IdSerializer<>(EdgeId.class));
        addDeserializer(EdgeId.class, new IdDeserializer<>(EdgeId.class));

        addSerializer(PropertyKey.class, new PropertyKeySerializer());
        addDeserializer(PropertyKey.class, new PropertyKeyDeserializer());
        addSerializer(VertexLabel.class, new VertexLabelSerializer());
        addDeserializer(VertexLabel.class, new VertexLabelDeserializer());
        addSerializer(EdgeLabel.class, new EdgeLabelSerializer());
        addDeserializer(EdgeLabel.class, new EdgeLabelDeserializer());
        addSerializer(IndexLabel.class, new IndexLabelSerializer());
        addDeserializer(IndexLabel.class, new IndexLabelDeserializer());

        addSerializer(Optional.class, new OptionalSerializer());
        addDeserializer(Optional.class, new OptionalDeserializer());
    }

    @Override
    @SuppressWarnings("rawtypes")
    public Map<Class, String> getTypeDefinitions() {
        return TYPE_DEFINITIONS;
    }

    @Override
    public String getTypeNamespace() {
        return TYPE_NAMESPACE;
    }

    private static class GraphSONSerializer extends TextSerializer {

        private final HugeGraph graph;

        public GraphSONSerializer(HugeGraph graph) {
            this.graph = graph;
        }

        private String getSchema(Id id, HugeType type) {
            switch (type) {
                case PROPERTY_KEY:
                    return graph.propertyKey(id).name();
                case VERTEX_LABEL:
                    return graph.vertexLabel(id).name();
                case EDGE_LABEL:
                    return graph.edgeLabel(id).name();
                case INDEX_LABEL:
                    return graph.indexLabel(id).name();
                default:
                    throw new AssertionError(
                              String.format("Unknown schema type '%s'", type));
            }
        }

        @Override
        protected String writeId(Id id, HugeType type) {
            assert SchemaElement.isSchema(type);
            return JsonUtil.toJson(this.getSchema(id, type));
        }

        @Override
        protected String writeIds(Collection<Id> ids, HugeType type) {
            assert SchemaElement.isSchema(type);
            List<String> names = new ArrayList<>(ids.size());
            for (Id id : ids) {
                names.add(this.getSchema(id, type));
            }
            return JsonUtil.toJson(names);
        }
    }

    @SuppressWarnings("rawtypes")
    private class OptionalSerializer extends StdSerializer<Optional> {

        public OptionalSerializer() {
            super(Optional.class);
        }

        @Override
        public void serialize(Optional optional,
                              JsonGenerator jsonGenerator,
                              SerializerProvider serializer)
                              throws IOException {
            if (optional.isPresent()) {
                jsonGenerator.writeObject(optional.get());
            } else {
                jsonGenerator.writeObject(null);
            }
        }
    }

    private class OptionalDeserializer extends StdDeserializer<Optional<?>> {

        public OptionalDeserializer() {
            super(Optional.class);
        }

        @Override
        public Optional<?> deserialize(JsonParser jsonParser,
                                       DeserializationContext context)
                                       throws IOException {
            return null;
        }
    }

    private class IdSerializer<T extends Id> extends StdSerializer<T> {

        public IdSerializer(Class<T> clazz) {
            super(clazz);
        }

        @Override
        public void serialize(T value,
                              JsonGenerator jsonGenerator,
                              SerializerProvider serializer)
                              throws IOException {
            if (value.number()) {
                jsonGenerator.writeNumber(value.asLong());
            } else {
                jsonGenerator.writeString(value.asString());
            }
        }

        @Override
        public void serializeWithType(T value,
                                      JsonGenerator jsonGenerator,
                                      SerializerProvider serializers,
                                      TypeSerializer typeSer)
                                      throws IOException {
            typeSer.writeTypePrefixForScalar(value, jsonGenerator);
            this.serialize(value, jsonGenerator, serializers);
            typeSer.writeTypeSuffixForScalar(value, jsonGenerator);
        }
    }

    @SuppressWarnings("unchecked")
    private class IdDeserializer<T extends Id> extends StdDeserializer<T> {

        public IdDeserializer(Class<T> clazz) {
            super(clazz);
        }

        @Override
        public T deserialize(JsonParser jsonParser,
                             DeserializationContext ctxt)
                             throws IOException {
            Class<?> clazz = this.handledType();
            if (clazz.equals(IdGenerator.LongId.class)) {
                Number idValue = ctxt.readValue(jsonParser, Number.class);
                return (T) IdGenerator.of(idValue.longValue());
            } else if (clazz.equals(IdGenerator.StringId.class)) {
                String idValue = ctxt.readValue(jsonParser, String.class);
                return (T) IdGenerator.of(idValue);
            } else {
                assert clazz.equals(EdgeId.class);
                String idValue = ctxt.readValue(jsonParser, String.class);
                return (T) EdgeId.parse(idValue);
            }
        }
    }

    private static void writeEntry(JsonGenerator jsonGenerator,
                                   TextBackendEntry entry)
                                   throws IOException {
        jsonGenerator.writeStartObject();
        // Write id
        jsonGenerator.writeNumberField(HugeKeys.ID.string(),
                                       entry.id().asLong());

        // Write columns size and data
        for (String name : entry.columnNames()) {
            jsonGenerator.writeFieldName(name);
            jsonGenerator.writeRawValue(entry.column(name));
        }
        jsonGenerator.writeEndObject();
    }

    private class PropertyKeySerializer extends StdSerializer<PropertyKey> {

        public PropertyKeySerializer() {
            super(PropertyKey.class);
        }

        @Override
        public void serialize(PropertyKey propertyKey,
                              JsonGenerator jsonGenerator,
                              SerializerProvider serializer)
                              throws IOException {
            BackendEntry entry = textSerializer.writePropertyKey(propertyKey);
            writeEntry(jsonGenerator, (TextBackendEntry) entry);
        }
    }

    private class PropertyKeyDeserializer extends StdDeserializer<PropertyKey> {

        public PropertyKeyDeserializer() {
            super(PropertyKey.class);
        }

        @Override
        public PropertyKey deserialize(JsonParser jsonParser,
                                       DeserializationContext ctxt)
                                       throws IOException {
            return null;
        }
    }

    private class VertexLabelSerializer extends StdSerializer<VertexLabel> {

        public VertexLabelSerializer() {
            super(VertexLabel.class);
        }

        @Override
        public void serialize(VertexLabel vertexLabel,
                              JsonGenerator jsonGenerator,
                              SerializerProvider serializer)
                              throws IOException {
            BackendEntry entry = textSerializer.writeVertexLabel(vertexLabel);
            writeEntry(jsonGenerator, (TextBackendEntry) entry);
        }

        @Override
        public void serializeWithType(VertexLabel vertexLabel,
                                      JsonGenerator jsonGenerator,
                                      SerializerProvider serializer,
                                      TypeSerializer typeSer)
                                      throws IOException {
            BackendEntry entry = textSerializer.writeVertexLabel(vertexLabel);
            writeEntry(jsonGenerator, (TextBackendEntry) entry);
        }
    }

    private class VertexLabelDeserializer extends StdDeserializer<VertexLabel> {

        public VertexLabelDeserializer() {
            super(VertexLabel.class);
        }

        @Override
        public VertexLabel deserialize(JsonParser jsonParser,
                                       DeserializationContext ctxt)
                                       throws IOException {
            return null;
        }

    }

    private class EdgeLabelSerializer extends StdSerializer<EdgeLabel> {

        public EdgeLabelSerializer() {
            super(EdgeLabel.class);
        }

        @Override
        public void serialize(EdgeLabel edgeLabel,
                              JsonGenerator jsonGenerator,
                              SerializerProvider serializer)
                              throws IOException {
            BackendEntry entry = textSerializer.writeEdgeLabel(edgeLabel);
            writeEntry(jsonGenerator, (TextBackendEntry) entry);
        }
    }

    private class EdgeLabelDeserializer extends StdDeserializer<EdgeLabel> {

        public EdgeLabelDeserializer() {
            super(EdgeLabel.class);
        }

        @Override
        public EdgeLabel deserialize(JsonParser jsonParser,
                                     DeserializationContext ctxt)
                                     throws IOException {
            return null;
        }
    }

    private class IndexLabelSerializer extends StdSerializer<IndexLabel> {

        public IndexLabelSerializer() {
            super(IndexLabel.class);
        }

        @Override
        public void serialize(IndexLabel indexLabel,
                              JsonGenerator jsonGenerator,
                              SerializerProvider serializer)
                              throws IOException {
            BackendEntry entry = textSerializer.writeIndexLabel(indexLabel);
            writeEntry(jsonGenerator, (TextBackendEntry) entry);
        }
    }

    private class IndexLabelDeserializer extends StdDeserializer<IndexLabel> {

        public IndexLabelDeserializer() {
            super(IndexLabel.class);
        }

        @Override
        public IndexLabel deserialize(JsonParser jsonParser,
                                      DeserializationContext ctxt)
                                      throws IOException {
            return null;
        }
    }
}
