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

package com.baidu.hugegraph.job.algorithm.comm;

import java.util.Map;

import com.baidu.hugegraph.job.algorithm.AbstractAlgorithm;
import com.baidu.hugegraph.traversal.algorithm.HugeTraverser;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.ParameterUtil;

public abstract class AbstractCommAlgorithm extends AbstractAlgorithm {

    private static final int MAX_TIMES = 2048;

    @Override
    public String category() {
        return CATEGORY_COMM;
    }

    protected static int times(Map<String, Object> parameters) {
        if (!parameters.containsKey(KEY_TIMES)) {
            return (int) DEFAULT_TIMES;
        }
        int times = ParameterUtil.parameterInt(parameters, KEY_TIMES);
        HugeTraverser.checkPositiveOrNoLimit(times, KEY_TIMES);
        E.checkArgument(times <= MAX_TIMES,
                        "The maximum number of iterations is %s, but got %s",
                        MAX_TIMES, times);
        return times;
    }

    protected static int stableTimes(Map<String, Object> parameters) {
        if (!parameters.containsKey(KEY_STABLE_TIMES)) {
            return (int) DEFAULT_STABLE_TIMES;
        }
        int times = ParameterUtil.parameterInt(parameters, KEY_STABLE_TIMES);
        HugeTraverser.checkPositiveOrNoLimit(times, KEY_STABLE_TIMES);
        E.checkArgument(times <= MAX_TIMES,
                        "The maximum number of stable iterations is %s, " +
                        "but got %s", MAX_TIMES, times);
        return times;
    }

    protected static double precision(Map<String, Object> parameters) {
        if (!parameters.containsKey(KEY_PRECISION)) {
            return DEFAULT_PRECISION;
        }
        double precision = ParameterUtil.parameterDouble(parameters,
                                                         KEY_PRECISION);
        E.checkArgument(0d < precision && precision < 1d,
                        "The %s parameter must be in range(0,1), but got: %s",
                        KEY_PRECISION, precision);
        return precision;
    }

    protected static String showCommunity(Map<String, Object> parameters) {
        if (!parameters.containsKey(KEY_SHOW_COMM)) {
            return null;
        }
        return ParameterUtil.parameterString(parameters, KEY_SHOW_COMM);
    }
}
