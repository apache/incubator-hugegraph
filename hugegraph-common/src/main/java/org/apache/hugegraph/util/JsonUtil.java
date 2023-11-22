/*
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

package org.apache.hugegraph.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hugegraph.rest.SerializeException;

import java.io.IOException;

public final class JsonUtil {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void registerModule(Module module) {
        MAPPER.registerModule(module);
    }

    public static String toJson(Object object) {
        try {
            return MAPPER.writeValueAsString(object);
        } catch (JsonProcessingException e) {
            throw new SerializeException("Failed to serialize object '%s'",
                                         e, object);
        }
    }

    public static <T> T fromJson(String json, Class<T> clazz) {
        try {
            return MAPPER.readValue(json, clazz);
        } catch (IOException e) {
            throw new SerializeException("Failed to deserialize json '%s'",
                                         e, json);
        }
    }

    public static <T> T convertValue(JsonNode node, Class<T> clazz) {
        try {
            return MAPPER.convertValue(node, clazz);
        } catch (IllegalArgumentException e) {
            throw new SerializeException("Failed to deserialize json node '%s'",
                                         e, node);
        }
    }
}
