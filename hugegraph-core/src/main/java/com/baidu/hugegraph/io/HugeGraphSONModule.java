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
import java.util.Map;

import org.apache.tinkerpop.gremlin.structure.io.graphson.TinkerPopJacksonModule;
import org.apache.tinkerpop.shaded.jackson.core.JsonGenerator;
import org.apache.tinkerpop.shaded.jackson.core.JsonParser;
import org.apache.tinkerpop.shaded.jackson.core.JsonProcessingException;
import org.apache.tinkerpop.shaded.jackson.databind.DeserializationContext;
import org.apache.tinkerpop.shaded.jackson.databind.SerializerProvider;
import org.apache.tinkerpop.shaded.jackson.databind.deser.std.StdDeserializer;
import org.apache.tinkerpop.shaded.jackson.databind.ser.std.StdSerializer;

import com.baidu.hugegraph.backend.serializer.TextBackendEntry;
import com.baidu.hugegraph.backend.serializer.TextSerializer;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.schema.EdgeLabel;
import com.baidu.hugegraph.schema.IndexLabel;
import com.baidu.hugegraph.schema.PropertyKey;
import com.baidu.hugegraph.schema.VertexLabel;
import com.baidu.hugegraph.type.define.HugeKeys;
import com.google.common.collect.ImmutableMap;


public class HugeGraphSONModule extends TinkerPopJacksonModule {

    private static final long serialVersionUID = 6480426922914059122L;

    private static final String TYPE_NAMESPACE = "hugegraph";

    private static TextSerializer textSerializer = new TextSerializer(null);

    @SuppressWarnings("rawtypes")
    private static final Map<Class, String> TYPE_DEFINITIONS = ImmutableMap.of(
            PropertyKey.class, "PropertyKey",
            VertexLabel.class, "VertexLabel",
            EdgeLabel.class, "EdgeLabel",
            IndexLabel.class, "IndexLabel");

    private static final HugeGraphSONModule INSTANCE = new HugeGraphSONModule();

    public static final HugeGraphSONModule getInstance() {
        return INSTANCE;
    }

    public HugeGraphSONModule() {
        super(TYPE_NAMESPACE);

        addSerializer(PropertyKey.class, new PropertyKeySerializer());
        addDeserializer(PropertyKey.class, new PropertyKeyDeserializer());
        addSerializer(VertexLabel.class, new VertexLabelSerializer());
        addDeserializer(VertexLabel.class, new VertexLabelDeserializer());
        addSerializer(EdgeLabel.class, new EdgeLabelSerializer());
        addDeserializer(EdgeLabel.class, new EdgeLabelDeserializer());
        addSerializer(IndexLabel.class, new IndexLabelSerializer());
        addDeserializer(IndexLabel.class, new IndexLabelDeserializer());
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

    private static void writeEntry(JsonGenerator jsonGenerator,
                                   TextBackendEntry entry)
                                   throws IOException {
        jsonGenerator.writeStartObject();
        // Write id
        jsonGenerator.writeStringField(HugeKeys.ID.string(),
                                       entry.id().asString());

        // Write columns size and data
        for (String name : entry.columnNames()) {
            jsonGenerator.writeFieldName(name);
            jsonGenerator.writeRawValue(entry.column(name));
        }
        jsonGenerator.writeEndObject();
    }

    @SuppressWarnings("serial")
    private class PropertyKeySerializer extends StdSerializer<PropertyKey> {

        public PropertyKeySerializer() {
            super(PropertyKey.class);
        }

        @Override
        public void serialize(PropertyKey propertyKey,
                              JsonGenerator jsonGenerator,
                              SerializerProvider serializerProvider)
                              throws IOException {
            BackendEntry entry = textSerializer.writePropertyKey(propertyKey);
            writeEntry(jsonGenerator, (TextBackendEntry) entry);
        }
    }

    @SuppressWarnings("serial")
    private class PropertyKeyDeserializer extends StdDeserializer<PropertyKey> {

        public PropertyKeyDeserializer() {
            super(PropertyKey.class);
        }

        @Override
        public PropertyKey deserialize(
                JsonParser jsonParser,
                DeserializationContext deserializationContext)
                throws IOException, JsonProcessingException {
            return null;
        }
    }

    @SuppressWarnings("serial")
    private class VertexLabelSerializer extends StdSerializer<VertexLabel> {

        public VertexLabelSerializer() {
            super(VertexLabel.class);
        }

        @Override
        public void serialize(VertexLabel vertexLabel,
                              JsonGenerator jsonGenerator,
                              SerializerProvider serializerProvider)
                              throws IOException {
            BackendEntry entry = textSerializer.writeVertexLabel(vertexLabel);
            writeEntry(jsonGenerator, (TextBackendEntry) entry);
        }

    }

    @SuppressWarnings("serial")
    private class VertexLabelDeserializer extends StdDeserializer<VertexLabel> {

        public VertexLabelDeserializer() {
            super(VertexLabel.class);
        }

        @Override
        public VertexLabel deserialize(
                JsonParser jsonParser,
                DeserializationContext deserializationContext)
                throws IOException, JsonProcessingException {

            return null;
        }

    }

    @SuppressWarnings("serial")
    private class EdgeLabelSerializer extends StdSerializer<EdgeLabel> {

        public EdgeLabelSerializer() {
            super(EdgeLabel.class);
        }

        @Override
        public void serialize(EdgeLabel edgeLabel,
                              JsonGenerator jsonGenerator,
                              SerializerProvider serializerProvider)
                throws IOException {
            BackendEntry entry = textSerializer.writeEdgeLabel(edgeLabel);
            writeEntry(jsonGenerator, (TextBackendEntry) entry);
        }
    }

    @SuppressWarnings("serial")
    private class EdgeLabelDeserializer extends StdDeserializer<EdgeLabel> {

        public EdgeLabelDeserializer() {
            super(EdgeLabel.class);
        }

        @Override
        public EdgeLabel deserialize(
                JsonParser jsonParser,
                DeserializationContext deserializationContext)
                throws IOException, JsonProcessingException {
            return null;
        }
    }

    @SuppressWarnings("serial")
    private class IndexLabelSerializer extends StdSerializer<IndexLabel> {

        public IndexLabelSerializer() {
            super(IndexLabel.class);
        }

        @Override
        public void serialize(IndexLabel indexLabel,
                              JsonGenerator jsonGenerator,
                              SerializerProvider serializerProvider)
                              throws IOException {
            BackendEntry entry = textSerializer.writeIndexLabel(indexLabel);
            writeEntry(jsonGenerator, (TextBackendEntry) entry);
        }
    }

    @SuppressWarnings("serial")
    private class IndexLabelDeserializer extends StdDeserializer<IndexLabel> {

        public IndexLabelDeserializer() {
            super(IndexLabel.class);
        }

        @Override
        public IndexLabel deserialize(
                JsonParser jsonParser,
                DeserializationContext deserializationContext)
                throws IOException, JsonProcessingException {
            return null;
        }
    }
}
