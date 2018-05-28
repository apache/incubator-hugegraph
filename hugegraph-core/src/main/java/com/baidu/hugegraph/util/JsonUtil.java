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

import com.baidu.hugegraph.backend.BackendException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

public final class JsonUtil {

    private static final ObjectMapper mapper = new ObjectMapper();

    static {
        SimpleModule module = new SimpleModule();
        module.addSerializer(Date.class, new DateSerializer());
        module.addDeserializer(Date.class, new DateDeserializer());
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
     * Collection<Number> will be parsed to Collection<Double> by fromJson,
     * this method used to cast element in collection to original number type
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
        public void serialize(Date date, JsonGenerator jsonGenerator,
                              SerializerProvider provider)
                              throws IOException {
            jsonGenerator.writeNumber(date.getTime());
        }
    }

    private static class DateDeserializer extends StdDeserializer<Date> {

        private static final long serialVersionUID = 1209944821349424949L;

        public DateDeserializer() {
            super(Date.class);
        }

        @Override
        public Date deserialize(JsonParser jsonParser,
                                DeserializationContext context)
                                throws IOException {
            Long number = jsonParser.readValueAs(Long.class);
            return new Date(number);
        }
    }
}
