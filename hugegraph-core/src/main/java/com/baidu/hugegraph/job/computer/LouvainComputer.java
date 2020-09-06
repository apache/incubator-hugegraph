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

import com.baidu.hugegraph.traversal.algorithm.HugeTraverser;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.ParameterUtil;
import com.google.common.collect.ImmutableMap;

public class LouvainComputer extends AbstractComputer {

    public static final String LOUVAIN = "louvain";

    public static final String KEY_STABLE_TIMES = "stable_times";
    public static final String KEY_PRECISION = "precision";
    public static final String KEY_SHOW_MOD= "show_modularity";
    public static final String KEY_SHOW_COMM = "show_community";
    public static final String KEY_EXPORT_COMM = "export_community";
    public static final String KEY_SKIP_ISOLATED = "skip_isolated";
    public static final String KEY_CLEAR = "clear";

    public static final long DEFAULT_STABLE_TIMES= 3L;
    private static final int MAX_TIMES = 2048;

    @Override
    public String name() {
        return LOUVAIN;
    }

    @Override
    public String category() {
        return CATEGORY_COMM;
    }

    @Override
    public void checkParameters(Map<String, Object> parameters) {
        times(parameters);
        stableTimes(parameters);
        precision(parameters);
        degree(parameters);
        showModularity(parameters);
        showCommunity(parameters);
        exportCommunity(parameters);
        skipIsolated(parameters);
        clearPass(parameters);
    }

    @Override
    protected Map<String, Object> checkAndCollectParameters(
            Map<String, Object> parameters) {
        return ImmutableMap.of(TIMES, times(parameters),
                               PRECISION, precision(parameters),
                               DIRECTION, direction(parameters),
                               DEGREE, degree(parameters));
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

    protected static Long showModularity(Map<String, Object> parameters) {
        if (!parameters.containsKey(KEY_SHOW_MOD)) {
            return null;
        }
        long pass = ParameterUtil.parameterLong(parameters, KEY_SHOW_MOD);
        HugeTraverser.checkNonNegative(pass, KEY_SHOW_MOD);
        return pass;
    }

    protected static String showCommunity(Map<String, Object> parameters) {
        if (!parameters.containsKey(KEY_SHOW_COMM)) {
            return null;
        }
        return ParameterUtil.parameterString(parameters, KEY_SHOW_COMM);
    }

    protected static Long exportCommunity(Map<String, Object> parameters) {
        if (!parameters.containsKey(KEY_EXPORT_COMM)) {
            return null;
        }
        long pass = ParameterUtil.parameterLong(parameters, KEY_EXPORT_COMM);
        HugeTraverser.checkNonNegative(pass, KEY_EXPORT_COMM);
        return pass;
    }

    protected static boolean skipIsolated(Map<String, Object> parameters) {
        if (!parameters.containsKey(KEY_SKIP_ISOLATED)) {
            return true;
        }
        return ParameterUtil.parameterBoolean(parameters, KEY_SKIP_ISOLATED);
    }

    protected static Long clearPass(Map<String, Object> parameters) {
        if (!parameters.containsKey(KEY_CLEAR)) {
            return null;
        }
        long pass = ParameterUtil.parameterLong(parameters, KEY_CLEAR);
        HugeTraverser.checkNonNegativeOrNoLimit(pass, KEY_CLEAR);
        return pass;
    }
}
