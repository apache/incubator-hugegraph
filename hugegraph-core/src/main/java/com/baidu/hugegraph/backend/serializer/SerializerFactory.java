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

package com.baidu.hugegraph.backend.serializer;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.baidu.hugegraph.backend.BackendException;

public class SerializerFactory {

    private static Map<String, Class<? extends AbstractSerializer>> serializers;

    static {
        serializers = new ConcurrentHashMap<>();
    }

    public static AbstractSerializer serializer(String name) {
        name = name.toLowerCase();
        if (name.equals("binary")) {
            return new BinarySerializer();
        } else if (name.equals("binaryinline")) {
            return new BinaryInlineSerializer();
        } else if (name.equals("text")) {
            return new TextSerializer();
        }

        Class<? extends AbstractSerializer> clazz = serializers.get(name);
        if (clazz == null) {
            throw new BackendException("Not exists serializer: %s", name);
        }

        assert AbstractSerializer.class.isAssignableFrom(clazz);
        try {
            return clazz.getConstructor().newInstance();
        } catch (Exception e) {
            throw new BackendException(e);
        }
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public static void register(String name, String classPath) {
        ClassLoader classLoader = SerializerFactory.class.getClassLoader();
        Class<?> clazz;
        try {
            clazz = classLoader.loadClass(classPath);
        } catch (Exception e) {
            throw new BackendException(e);
        }

        // Check subclass
        if (!AbstractSerializer.class.isAssignableFrom(clazz)) {
            throw new BackendException("Class '%s' is not a subclass of " +
                                       "class AbstractSerializer", classPath);
        }

        // Check exists
        if (serializers.containsKey(name)) {
            throw new BackendException("Exists serializer: %s(Class '%s')",
                                       name, serializers.get(name).getName());
        }

        // Register class
        serializers.put(name, (Class) clazz);
    }
}
