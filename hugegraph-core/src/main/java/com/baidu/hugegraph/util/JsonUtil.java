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

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.io.HugeGraphSONModule;

public final class JsonUtil {

    private static final ObjectMapper mapper = new ObjectMapper();

    static {
        SimpleModule module = new SimpleModule();

        module.addSerializer(RawJson.class, new RawJsonSerializer());

        module.addSerializer(Date.class, new DateSerializer());
        module.addDeserializer(Date.class, new DateDeserializer());

        module.addSerializer(UUID.class, new UUIDSerializer());
        module.addDeserializer(UUID.class, new UUIDDeserializer());

        HugeGraphSONModule.registerIdSerializers(module);
        HugeGraphSONModule.registerSchemaSerializers(module);
        HugeGraphSONModule.registerGraphSerializers(module);

        mapper.registerModule(module);
    }

    public static void registerModule(Module module) {
        mapper.registerModule(module);
    }

    public static String toJson(Object object) {
        try {
            return mapper.writeValueAsString(object);
        } catch (JsonProcessingException e) {
            throw new HugeException("Can't write json: %s", e, e.getMessage());
        }
    }

    public static <T> T fromJson(String json, Class<T> clazz) {
        E.checkState(json != null,
                     "Json value can't be null for '%s'",
                     clazz.getSimpleName());
        try {
            return mapper.readValue(json, clazz);
        } catch (IOException e) {
            throw new HugeException("Can't read json: %s", e, e.getMessage());
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
            throw new HugeException("Can't read json: %s", e, e.getMessage());
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
            } else if (clazz == Double.class) {
                assert object instanceof Double : object;
            } else {
                assert clazz == Date.class : clazz;
            }
        }
        return object;
    }

    public static Object asJson(Object value) {
        return new RawJson(toJson(value));
    }

    public static Object asJson(String value) {
        return new RawJson(value);
    }

    private static class RawJson {

        private final String value;

        public RawJson(String value) {
            this.value = value;
        }

        public String value() {
            return this.value;
        }
    }

    private static class RawJsonSerializer extends StdSerializer<RawJson> {

        private static final long serialVersionUID = 3240301861031054251L;

        public RawJsonSerializer() {
            super(RawJson.class);
        }

        @Override
        public void serialize(RawJson json, JsonGenerator generator,
                              SerializerProvider provider)
                              throws IOException {
            generator.writeRaw(json.value());
        }
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
