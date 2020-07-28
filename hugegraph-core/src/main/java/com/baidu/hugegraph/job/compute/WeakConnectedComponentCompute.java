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

package com.baidu.hugegraph.job.compute;

import java.util.Map;

import com.baidu.hugegraph.util.E;

public class WeakConnectedComponentCompute extends AbstractCompute {

    public static final String WCC = "weak_connected_component";

    public static final String PRECISION = "precision";
    public static final double DEFAULT_PRECISION = 0.0001D;

    @Override
    public String name() {
        return WCC;
    }

    @Override
    public String category() {
        return CATEGORY_COMM;
    }

    @Override
    public void checkParameters(Map<String, Object> parameters) {
        this.userDefinedParameters.put(MAX_STEPS, maxSteps(parameters));
        this.userDefinedParameters.put(PRECISION, threshold(parameters));
    }

    private static double threshold(Map<String, Object> parameters) {
        if (!parameters.containsKey(PRECISION)) {
            return DEFAULT_PRECISION;
        }
        double precision = parameterDouble(parameters, PRECISION);
        E.checkArgument(precision > 0 && precision < 1,
                        "The value of %s must be (0, 1), but got %s",
                        PRECISION, precision);
        return precision;
    }
}
