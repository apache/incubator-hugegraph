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

import java.util.Map;

public class ParameterUtil {

    public static Object parameter(Map<String, Object> parameters, String key) {
        Object value = parameters.get(key);
        E.checkArgument(value != null,
                        "Expect '%s' in parameters: %s",
                        key, parameters);
        return value;
    }

    public static String parameterString(Map<String, Object> parameters,
                                         String key) {
        Object value = parameter(parameters, key);
        E.checkArgument(value instanceof String,
                        "Expect string value for parameter '%s': '%s'",
                        key, value);
        return (String) value;
    }

    public static int parameterInt(Map<String, Object> parameters,
                                   String key) {
        Object value = parameter(parameters, key);
        E.checkArgument(value instanceof Number,
                        "Expect int value for parameter '%s': '%s'",
                        key, value);
        return ((Number) value).intValue();
    }

    public static long parameterLong(Map<String, Object> parameters,
                                     String key) {
        Object value = parameter(parameters, key);
        E.checkArgument(value instanceof Number,
                        "Expect long value for parameter '%s': '%s'",
                        key, value);
        return ((Number) value).longValue();
    }

    public static double parameterDouble(Map<String, Object> parameters,
                                         String key) {
        Object value = parameter(parameters, key);
        E.checkArgument(value instanceof Number,
                        "Expect double value for parameter '%s': '%s'",
                        key, value);
        return ((Number) value).doubleValue();
    }

    public static boolean parameterBoolean(Map<String, Object> parameters,
                                           String key) {
        Object value = parameter(parameters, key);
        E.checkArgument(value instanceof Boolean,
                        "Expect boolean value for parameter '%s': '%s'",
                        key, value);
        return ((Boolean) value);
    }
}
