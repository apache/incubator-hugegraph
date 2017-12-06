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
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.tinkerpop.gremlin.structure.io.graphson.TinkerPopJacksonModule;
import org.apache.tinkerpop.shaded.jackson.core.JsonGenerator;
import org.apache.tinkerpop.shaded.jackson.core.JsonParser;
import org.apache.tinkerpop.shaded.jackson.databind.DeserializationContext;
import org.apache.tinkerpop.shaded.jackson.databind.SerializerProvider;
import org.apache.tinkerpop.shaded.jackson.databind.deser.std.StdDeserializer;
import org.apache.tinkerpop.shaded.jackson.databind.jsontype.TypeSerializer;
import org.apache.tinkerpop.shaded.jackson.databind.ser.std.StdSerializer;

import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.backend.serializer.TextBackendEntry;
import com.baidu.hugegraph.backend.serializer.TextSerializer;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.schema.EdgeLabel;
import com.baidu.hugegraph.schema.IndexLabel;
import com.baidu.hugegraph.schema.PropertyKey;
import com.baidu.hugegraph.schema.VertexLabel;
import com.baidu.hugegraph.type.define.HugeKeys;

@SuppressWarnings("serial")
public class HugeGraphSONModule extends TinkerPopJacksonModule {

    private static final long serialVersionUID = 6480426922914059122L;

    private static final String TYPE_NAMESPACE = "hugegraph";

    private static TextSerializer textSerializer = new TextSerializer();

    @SuppressWarnings("rawtypes")
    private static final Map<Class, String> TYPE_DEFINITIONS;

    static {
        TYPE_DEFINITIONS = new HashMap<>();
        TYPE_DEFINITIONS.put(Optional.class, "Optional");
        // HugeGraph releated serializer
        TYPE_DEFINITIONS.put(IdGenerator.StringId.class, "StringId");
        TYPE_DEFINITIONS.put(IdGenerator.LongId.class, "LongId");
        TYPE_DEFINITIONS.put(PropertyKey.class, "PropertyKey");
        TYPE_DEFINITIONS.put(VertexLabel.class, "VertexLabel");
        TYPE_DEFINITIONS.put(EdgeLabel.class, "EdgeLabel");
        TYPE_DEFINITIONS.put(IndexLabel.class, "IndexLabel");
    }

    private static final HugeGraphSONModule INSTANCE = new HugeGraphSONModule();

    public static final HugeGraphSONModule getInstance() {
        return INSTANCE;
    }

    public HugeGraphSONModule() {
        super(TYPE_NAMESPACE);

        addSerializer(Id.class, new IdSerializer());
        addDeserializer(Id.class, new IdDeserializer());

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

    private class IdSerializer extends StdSerializer<Id> {

        public IdSerializer() {
            super(Id.class);
        }

        @Override
        public void serialize(Id id, JsonGenerator jsonGenerator,
                              SerializerProvider serializer)
                              throws IOException {
            jsonGenerator.writeString(id.asString());
        }

        @Override
        public void serializeWithType(Id id, JsonGenerator jsonGenerator,
                                      SerializerProvider serializers,
                                      TypeSerializer typeSer)
                                      throws IOException {
            typeSer.writeTypePrefixForScalar(id, jsonGenerator);
            jsonGenerator.writeString(id.asString());
            typeSer.writeTypeSuffixForScalar(id, jsonGenerator);
        }

    }

    private class IdDeserializer extends StdDeserializer<Id> {

        public IdDeserializer() {
            super(Id.class);
        }

        @Override
        public Id deserialize(JsonParser jsonParser,
                              DeserializationContext ctxt)
                              throws IOException {
            String idValue = ctxt.readValue(jsonParser, String.class);
            return IdGenerator.of(idValue);
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
