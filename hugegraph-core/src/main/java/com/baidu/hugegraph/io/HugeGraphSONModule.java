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
import java.util.Date;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONIo;
import org.apache.tinkerpop.gremlin.structure.io.graphson.TinkerPopJacksonModule;
import org.apache.tinkerpop.shaded.jackson.core.JsonGenerator;
import org.apache.tinkerpop.shaded.jackson.core.JsonParser;
import org.apache.tinkerpop.shaded.jackson.core.JsonToken;
import org.apache.tinkerpop.shaded.jackson.core.type.WritableTypeId;
import org.apache.tinkerpop.shaded.jackson.databind.DeserializationContext;
import org.apache.tinkerpop.shaded.jackson.databind.JsonSerializer;
import org.apache.tinkerpop.shaded.jackson.databind.SerializerProvider;
import org.apache.tinkerpop.shaded.jackson.databind.deser.std.StdDeserializer;
import org.apache.tinkerpop.shaded.jackson.databind.jsontype.TypeSerializer;
import org.apache.tinkerpop.shaded.jackson.databind.module.SimpleModule;
import org.apache.tinkerpop.shaded.jackson.databind.ser.std.DateSerializer;
import org.apache.tinkerpop.shaded.jackson.databind.ser.std.StdSerializer;
import org.apache.tinkerpop.shaded.jackson.databind.ser.std.UUIDSerializer;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.backend.id.EdgeId;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.backend.id.IdGenerator.LongId;
import com.baidu.hugegraph.backend.id.IdGenerator.StringId;
import com.baidu.hugegraph.backend.id.IdGenerator.UuidId;
import com.baidu.hugegraph.backend.store.Shard;
import com.baidu.hugegraph.date.SafeDateFormat;
import com.baidu.hugegraph.schema.EdgeLabel;
import com.baidu.hugegraph.schema.IndexLabel;
import com.baidu.hugegraph.schema.PropertyKey;
import com.baidu.hugegraph.schema.VertexLabel;
import com.baidu.hugegraph.structure.HugeEdge;
import com.baidu.hugegraph.structure.HugeElement;
import com.baidu.hugegraph.structure.HugeProperty;
import com.baidu.hugegraph.structure.HugeVertex;
import com.baidu.hugegraph.type.define.HugeKeys;

@SuppressWarnings("serial")
public class HugeGraphSONModule extends TinkerPopJacksonModule {

    private static final long serialVersionUID = 6480426922914059122L;

    public static boolean OPTIMIZE_SERIALIZE = true;

    private static final String TYPE_NAMESPACE = "hugegraph";

    @SuppressWarnings("rawtypes")
    private static final Map<Class, String> TYPE_DEFINITIONS;

    private static final GraphSONSchemaSerializer schemaSerializer =
                         new GraphSONSchemaSerializer();

    private static final String DF = "yyyy-MM-dd HH:mm:ss.SSS";
    private static final SafeDateFormat DATE_FORMAT = new SafeDateFormat(DF);

    static {
        TYPE_DEFINITIONS = new ConcurrentHashMap<>();

        TYPE_DEFINITIONS.put(Optional.class, "Optional");
        TYPE_DEFINITIONS.put(Date.class, "Date");
        TYPE_DEFINITIONS.put(UUID.class, "UUID");

        // HugeGraph id serializer
        TYPE_DEFINITIONS.put(IdGenerator.StringId.class, "StringId");
        TYPE_DEFINITIONS.put(IdGenerator.LongId.class, "LongId");
        TYPE_DEFINITIONS.put(EdgeId.class, "EdgeId");

        // HugeGraph schema serializer
        TYPE_DEFINITIONS.put(PropertyKey.class, "PropertyKey");
        TYPE_DEFINITIONS.put(VertexLabel.class, "VertexLabel");
        TYPE_DEFINITIONS.put(EdgeLabel.class, "EdgeLabel");
        TYPE_DEFINITIONS.put(IndexLabel.class, "IndexLabel");

        // HugeGraph vertex serializer
        TYPE_DEFINITIONS.put(HugeVertex.class, "HugeVertex");
        // TYPE_DEFINITIONS.put(HugeEdge.class, "HugeEdge");

        // HugeGraph shard serializer
        TYPE_DEFINITIONS.put(Shard.class, "Shard");
    }

    public static void register(HugeGraphIoRegistry io) {
        io.register(GraphSONIo.class, null, new HugeGraphSONModule());
    }

    private HugeGraphSONModule() {
        super(TYPE_NAMESPACE);

        addSerializer(Optional.class, new OptionalSerializer());

        addSerializer(Date.class, new DateSerializer(false, DATE_FORMAT));
        addSerializer(UUID.class, new UUIDSerializer());

        // HugeGraph id serializer
        registerIdSerializers(this);

        // HugeGraph schema serializer
        registerSchemaSerializers(this);

        // HugeGraph vertex/edge serializer
        if (OPTIMIZE_SERIALIZE) {
            registerGraphSerializers(this);
        }
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Map<Class, String> getTypeDefinitions() {
        return TYPE_DEFINITIONS;
    }

    @Override
    public String getTypeNamespace() {
        return TYPE_NAMESPACE;
    }

    public static void registerIdSerializers(SimpleModule module) {
        module.addSerializer(StringId.class,
                             new IdSerializer<>(StringId.class));
        module.addDeserializer(StringId.class,
                               new IdDeserializer<>(StringId.class));

        module.addSerializer(LongId.class,
                             new IdSerializer<>(LongId.class));
        module.addDeserializer(LongId.class,
                               new IdDeserializer<>(LongId.class));

        module.addSerializer(UuidId.class,
                             new IdSerializer<>(UuidId.class));
        module.addDeserializer(UuidId.class,
                               new IdDeserializer<>(UuidId.class));

        module.addSerializer(EdgeId.class,
                             new IdSerializer<>(EdgeId.class));
        module.addDeserializer(EdgeId.class,
                               new IdDeserializer<>(EdgeId.class));
    }

    public static void registerSchemaSerializers(SimpleModule module) {
        module.addSerializer(PropertyKey.class, new PropertyKeySerializer());
        module.addSerializer(VertexLabel.class, new VertexLabelSerializer());
        module.addSerializer(EdgeLabel.class, new EdgeLabelSerializer());
        module.addSerializer(IndexLabel.class, new IndexLabelSerializer());

        module.addSerializer(Shard.class, new ShardSerializer());
    }

    public static void registerGraphSerializers(SimpleModule module) {
        /*
         * Use customized serializer need to be compatible with V1 and V2
         * Graphson, and seems need to implement edge deserializer，it is
         * a little complicated.
         */
        module.addSerializer(HugeVertex.class, new HugeVertexSerializer());
        module.addSerializer(HugeEdge.class, new HugeEdgeSerializer());
    }

    @SuppressWarnings("rawtypes")
    private static class OptionalSerializer extends StdSerializer<Optional> {

        public OptionalSerializer() {
            super(Optional.class);
        }

        @Override
        public void serialize(Optional optional,
                              JsonGenerator jsonGenerator,
                              SerializerProvider provider)
                              throws IOException {
            if (optional.isPresent()) {
                jsonGenerator.writeObject(optional.get());
            } else {
                jsonGenerator.writeObject(null);
            }
        }
    }

    private static class IdSerializer<T extends Id> extends StdSerializer<T> {

        public IdSerializer(Class<T> clazz) {
            super(clazz);
        }

        @Override
        public void serialize(T value,
                              JsonGenerator jsonGenerator,
                              SerializerProvider provider)
                              throws IOException {
            if (value.number()) {
                jsonGenerator.writeNumber(value.asLong());
            } else {
                jsonGenerator.writeString(value.asString());
            }
        }

        @SuppressWarnings("deprecation")
        @Override
        public void serializeWithType(T value,
                                      JsonGenerator jsonGenerator,
                                      SerializerProvider provider,
                                      TypeSerializer typeSer)
                                      throws IOException {
            // https://github.com/FasterXML/jackson-databind/issues/2320
            WritableTypeId typeId = typeSer.typeId(value, JsonToken.VALUE_STRING);
            typeSer.writeTypePrefix(jsonGenerator, typeId);
            this.serialize(value, jsonGenerator, provider);
            typeSer.writeTypeSuffix(jsonGenerator, typeId);
        }
    }

    private static class IdDeserializer<T extends Id>
                   extends StdDeserializer<T> {

        public IdDeserializer(Class<T> clazz) {
            super(clazz);
        }

        @SuppressWarnings("unchecked")
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
            } else if (clazz.equals(IdGenerator.UuidId.class)) {
                UUID idValue = ctxt.readValue(jsonParser, UUID.class);
                return (T) IdGenerator.of(idValue);
            } else {
                assert clazz.equals(EdgeId.class);
                String idValue = ctxt.readValue(jsonParser, String.class);
                return (T) EdgeId.parse(idValue);
            }
        }
    }

    private static class PropertyKeySerializer
                   extends StdSerializer<PropertyKey> {

        public PropertyKeySerializer() {
            super(PropertyKey.class);
        }

        @Override
        public void serialize(PropertyKey pk,
                              JsonGenerator jsonGenerator,
                              SerializerProvider provider)
                              throws IOException {
            writeEntry(jsonGenerator, schemaSerializer.writePropertyKey(pk));
        }
    }

    private static class VertexLabelSerializer
                   extends StdSerializer<VertexLabel> {

        public VertexLabelSerializer() {
            super(VertexLabel.class);
        }

        @Override
        public void serialize(VertexLabel vl,
                              JsonGenerator jsonGenerator,
                              SerializerProvider provider)
                              throws IOException {
            writeEntry(jsonGenerator, schemaSerializer.writeVertexLabel(vl));
        }
    }

    private static class EdgeLabelSerializer extends StdSerializer<EdgeLabel> {

        public EdgeLabelSerializer() {
            super(EdgeLabel.class);
        }

        @Override
        public void serialize(EdgeLabel el,
                              JsonGenerator jsonGenerator,
                              SerializerProvider provider)
                              throws IOException {
            writeEntry(jsonGenerator, schemaSerializer.writeEdgeLabel(el));
        }
    }

    private static class IndexLabelSerializer
                   extends StdSerializer<IndexLabel> {

        public IndexLabelSerializer() {
            super(IndexLabel.class);
        }

        @Override
        public void serialize(IndexLabel il,
                              JsonGenerator jsonGenerator,
                              SerializerProvider provider)
                              throws IOException {
            writeEntry(jsonGenerator, schemaSerializer.writeIndexLabel(il));
        }
    }

    private static void writeEntry(JsonGenerator jsonGenerator,
                                   Map<HugeKeys, Object> schema)
                                   throws IOException {
        jsonGenerator.writeStartObject();
        for (Map.Entry<HugeKeys, Object> entry : schema.entrySet()) {
            jsonGenerator.writeFieldName(entry.getKey().string());
            jsonGenerator.writeObject(entry.getValue());
        }
        jsonGenerator.writeEndObject();
    }

    protected static abstract class HugeElementSerializer<T extends HugeElement>
                              extends StdSerializer<T> {

        public HugeElementSerializer(Class<T> clazz) {
            super(clazz);
        }

        public void writeIdField(String fieldName, Id id,
                                 JsonGenerator generator)
                                 throws IOException {
            generator.writeFieldName(fieldName);
            if (id.number()) {
                generator.writeNumber(id.asLong());
            } else {
                generator.writeString(id.asString());
            }
        }

        public void writePropertiesField(Map<Id, HugeProperty<?>> properties,
                                         JsonGenerator generator,
                                         SerializerProvider provider)
                                         throws IOException {
            // Start write properties
            generator.writeFieldName("properties");
            generator.writeStartObject();

            for (HugeProperty<?> property : properties.values()) {
                String key = property.key();
                Object val = property.value();
                try {
                    generator.writeFieldName(key);
                    if (val != null) {
                        JsonSerializer<Object> serializer =
                                provider.findValueSerializer(val.getClass());
                        serializer.serialize(val, generator, provider);
                    } else {
                        generator.writeNull();
                    }
                } catch (IOException e) {
                    throw new HugeException(
                              "Failed to serialize property(%s: %s) " +
                              "for vertex '%s'", key, val, property.element());
                }
            };
            // End wirte properties
            generator.writeEndObject();
        }
    }

    private static class HugeVertexSerializer
                   extends HugeElementSerializer<HugeVertex> {

        public HugeVertexSerializer() {
            super(HugeVertex.class);
        }

        @Override
        public void serialize(HugeVertex vertex, JsonGenerator generator,
                              SerializerProvider provider)
                              throws IOException {
            generator.writeStartObject();

            this.writeIdField("id", vertex.id(), generator);
            generator.writeStringField("label", vertex.label());
            generator.writeStringField("type", "vertex");

            this.writePropertiesField(vertex.getFilledProperties(),
                                      generator, provider);

            generator.writeEndObject();
        }
    }

    private static class HugeEdgeSerializer
                   extends HugeElementSerializer<HugeEdge> {

        public HugeEdgeSerializer() {
            super(HugeEdge.class);
        }

        @Override
        public void serialize(HugeEdge edge, JsonGenerator generator,
                              SerializerProvider provider)
                              throws IOException {
            generator.writeStartObject();

            // Write id, label, type
            this.writeIdField("id", edge.id(), generator);
            generator.writeStringField("label", edge.label());
            generator.writeStringField("type", "edge");

            HugeVertex outVertex = (HugeVertex) edge.outVertex();
            HugeVertex inVertex = (HugeVertex) edge.inVertex();
            this.writeIdField("outV", outVertex.id(), generator);
            generator.writeStringField("outVLabel", outVertex.label());
            this.writeIdField("inV", inVertex.id(), generator);
            generator.writeStringField("inVLabel", inVertex.label());

            this.writePropertiesField(edge.getFilledProperties(),
                                      generator, provider);

            generator.writeEndObject();
        }
    }

    private static class ShardSerializer extends StdSerializer<Shard> {

        public ShardSerializer() {
            super(Shard.class);
        }

        @Override
        public void serialize(Shard shard, JsonGenerator jsonGenerator,
                              SerializerProvider provider)
                              throws IOException {
            jsonGenerator.writeStartObject();
            jsonGenerator.writeStringField("start", shard.start());
            jsonGenerator.writeStringField("end", shard.end());
            jsonGenerator.writeNumberField("length", shard.length());
            jsonGenerator.writeEndObject();
        }
    }
}
