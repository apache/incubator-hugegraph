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

package com.baidu.hugegraph.util;

import java.io.IOException;
import java.util.Date;
import java.util.UUID;

import org.apache.tinkerpop.shaded.jackson.core.JsonGenerator;
import org.apache.tinkerpop.shaded.jackson.core.JsonParser;
import org.apache.tinkerpop.shaded.jackson.core.JsonProcessingException;
import org.apache.tinkerpop.shaded.jackson.core.type.TypeReference;
import org.apache.tinkerpop.shaded.jackson.databind.DeserializationContext;
import org.apache.tinkerpop.shaded.jackson.databind.Module;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectReader;
import org.apache.tinkerpop.shaded.jackson.databind.SerializerProvider;
import org.apache.tinkerpop.shaded.jackson.databind.deser.std.StdDeserializer;
import org.apache.tinkerpop.shaded.jackson.databind.deser.std.UUIDDeserializer;
import org.apache.tinkerpop.shaded.jackson.databind.module.SimpleModule;
import org.apache.tinkerpop.shaded.jackson.databind.ser.std.StdSerializer;
import org.apache.tinkerpop.shaded.jackson.databind.ser.std.UUIDSerializer;

import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.id.EdgeId;
import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.backend.store.Shard;
import com.baidu.hugegraph.io.HugeGraphSONModule.EdgeLabelSerializer;
import com.baidu.hugegraph.io.HugeGraphSONModule.HugeEdgeSerializer;
import com.baidu.hugegraph.io.HugeGraphSONModule.HugeVertexSerializer;
import com.baidu.hugegraph.io.HugeGraphSONModule.IdSerializer;
import com.baidu.hugegraph.io.HugeGraphSONModule.IndexLabelSerializer;
import com.baidu.hugegraph.io.HugeGraphSONModule.PropertyKeySerializer;
import com.baidu.hugegraph.io.HugeGraphSONModule.ShardSerializer;
import com.baidu.hugegraph.io.HugeGraphSONModule.VertexLabelSerializer;
import com.baidu.hugegraph.schema.EdgeLabel;
import com.baidu.hugegraph.schema.IndexLabel;
import com.baidu.hugegraph.schema.PropertyKey;
import com.baidu.hugegraph.schema.VertexLabel;
import com.baidu.hugegraph.structure.HugeEdge;
import com.baidu.hugegraph.structure.HugeVertex;

public final class JsonUtil {

    private static final ObjectMapper mapper = new ObjectMapper();

    static {
        SimpleModule module = new SimpleModule();

        module.addSerializer(Date.class, new DateSerializer());
        module.addDeserializer(Date.class, new DateDeserializer());

        module.addSerializer(UUID.class, new UUIDSerializer());
        module.addDeserializer(UUID.class, new UUIDDeserializer());

        module.addSerializer(IdGenerator.StringId.class,
                             new IdSerializer<>(IdGenerator.StringId.class));
        module.addSerializer(IdGenerator.LongId.class,
                             new IdSerializer<>(IdGenerator.LongId.class));
        module.addSerializer(IdGenerator.UuidId.class,
                             new IdSerializer<>(IdGenerator.UuidId.class));
        module.addSerializer(EdgeId.class, new IdSerializer<>(EdgeId.class));

        module.addSerializer(PropertyKey.class, new PropertyKeySerializer());
        module.addSerializer(VertexLabel.class, new VertexLabelSerializer());
        module.addSerializer(EdgeLabel.class, new EdgeLabelSerializer());
        module.addSerializer(IndexLabel.class, new IndexLabelSerializer());

        module.addSerializer(HugeVertex.class, new HugeVertexSerializer());
        module.addSerializer(HugeEdge.class, new HugeEdgeSerializer());

        module.addSerializer(Shard.class, new ShardSerializer());
        mapper.registerModule(module);
    }

    public static void registerModule(Module module) {
        mapper.registerModule(module);
    }

    public static String toJson(Object object) {
        try {
            return mapper.writeValueAsString(object);
        } catch (JsonProcessingException e) {
            throw new BackendException(e);
        }
    }

    public static <T> T fromJson(String json, Class<T> clazz) {
        E.checkState(json != null,
                     "Json value can't be null for '%s'",
                     clazz.getSimpleName());
        try {
            return mapper.readValue(json, clazz);
        } catch (IOException e) {
            throw new BackendException(e);
        }
    }

    public static <T> T fromJson(String json, TypeReference<?> typeRef) {
        E.checkState(json != null,
                     "Json value can't be null for '%s'",
                     typeRef.getType());
        try {
            ObjectReader reader = mapper.readerFor(typeRef);
            return reader.readValue(json);
        } catch (IOException e) {
            throw new BackendException(e);
        }
    }

    /**
     * Number collection will be parsed to Double Collection via fromJson,
     * this method used to cast element in collection to original number type
     * @param object    original number
     * @param clazz     target type
     * @return          target number
     */
    public static Object castNumber(Object object, Class<?> clazz) {
        if (object instanceof Number) {
            Number number = (Number) object;
            if (clazz == Byte.class) {
                object = number.byteValue();
            } else if (clazz == Integer.class) {
                object = number.intValue();
            } else if (clazz == Long.class) {
                object = number.longValue();
            } else if (clazz == Float.class) {
                object = number.floatValue();
            } else {
                assert clazz == Double.class;
            }
        }
        return object;
    }

    private static class DateSerializer extends StdSerializer<Date> {

        private static final long serialVersionUID = -6615155657857746161L;

        public DateSerializer() {
            super(Date.class);
        }

        @Override
        public void serialize(Date date, JsonGenerator generator,
                              SerializerProvider provider)
                              throws IOException {
            generator.writeNumber(date.getTime());
        }
    }

    private static class DateDeserializer extends StdDeserializer<Date> {

        private static final long serialVersionUID = 1209944821349424949L;

        public DateDeserializer() {
            super(Date.class);
        }

        @Override
        public Date deserialize(JsonParser parser,
                                DeserializationContext context)
                                throws IOException {
            Long number = parser.readValueAs(Long.class);
            return new Date(number);
        }
    }
}
