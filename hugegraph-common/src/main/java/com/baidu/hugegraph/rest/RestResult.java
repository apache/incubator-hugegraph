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

package com.baidu.hugegraph.rest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;

public class RestResult {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final int status;
    private final MultivaluedMap<String, Object> headers;
    private final String content;

    public RestResult(Response response) {
        this.status = response.getStatus();
        this.headers = response.getHeaders();
        this.content = response.readEntity(String.class);
    }

    public int status() {
        return this.status;
    }

    public MultivaluedMap<String, Object> headers() {
        return this.headers;
    }

    public String content() {
        return this.content;
    }

    public <T> T readObject(Class<T> clazz) {
        try {
            return MAPPER.readValue(this.content, clazz);
        } catch (Exception e) {
            throw new SerializeException(
                      "Failed to deserialize: %s", e, this.content);
        }
    }

    @SuppressWarnings("deprecation")
    public <T> List<T> readList(String key, Class<T> clazz) {
        try {
            JsonNode root = MAPPER.readTree(this.content);
            JsonNode element = root.get(key);
            if (element == null) {
                throw new SerializeException(
                          "Can't find value of the key: %s in json.", key);
            }
            JavaType type = MAPPER.getTypeFactory()
                                  .constructParametrizedType(ArrayList.class,
                                                             List.class, clazz);
            return MAPPER.convertValue(element, type);
        } catch (IOException e) {
            throw new SerializeException(
                      "Failed to deserialize %s", e, this.content);
        }
    }

    @SuppressWarnings("deprecation")
    public <T> List<T> readList(Class<T> clazz) {
        JavaType type = MAPPER.getTypeFactory()
                              .constructParametrizedType(ArrayList.class,
                                                         List.class, clazz);
        try {
            return MAPPER.readValue(this.content, type);
        } catch (IOException e) {
            throw new SerializeException(
                      "Failed to deserialize %s", e, this.content);
        }
    }

    @Override
    public String toString() {
        return String.format("{status=%s, headers=%s, content=%s}",
                             this.status, this.headers, this.content);
    }

    public static void registerModule(Module module) {
        MAPPER.registerModule(module);
    }
}
