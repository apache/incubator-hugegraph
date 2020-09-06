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

package com.baidu.hugegraph.job.computer;

import java.util.Map;

import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.ParameterUtil;
import com.google.common.collect.ImmutableMap;

public class LpaComputer extends AbstractComputer {

    public static final String LPA = "lpa";

    public static final String PROPERTY = "property";
    public static final String DEFAULT_PROPERTY = "id";

    @Override
    public String name() {
        return LPA;
    }

    @Override
    public String category() {
        return CATEGORY_COMM;
    }

    @Override
    public void checkParameters(Map<String, Object> parameters) {
        times(parameters);
        property(parameters);
        precision(parameters);
        direction(parameters);
        degree(parameters);
    }

    @Override
    protected Map<String, Object> checkAndCollectParameters(
            Map<String, Object> parameters) {
        return ImmutableMap.of(TIMES, times(parameters),
                               PROPERTY, property(parameters),
                               PRECISION, precision(parameters),
                               DIRECTION, direction(parameters),
                               DEGREE, degree(parameters));
    }

    private static String property(Map<String, Object> parameters) {
        if (!parameters.containsKey(PROPERTY)) {
            return DEFAULT_PROPERTY;
        }
        String property = ParameterUtil.parameterString(parameters, PROPERTY);
        E.checkArgument(property != null && !property.isEmpty(),
                        "The value of %s can not be null or empty", PROPERTY);
        return property;
    }
}
