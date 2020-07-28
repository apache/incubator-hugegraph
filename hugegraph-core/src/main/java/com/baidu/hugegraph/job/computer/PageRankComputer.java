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

public class PageRankComputer extends AbstractComputer {

    public static final String PAGE_RANK = "page_rank";

    public static final String ALPHA = "alpha";
    public static final double DEFAULT_ALPHA = 0.15D;

    @Override
    public String name() {
        return PAGE_RANK;
    }

    @Override
    public String category() {
        return CATEGORY_RANK;
    }

    @Override
    public void checkParameters(Map<String, Object> parameters) {
        maxSteps(parameters);
        alpha(parameters);
        precision(parameters);
    }

    @Override
    public Map<String, Object> checkAndCollectParameters(
                               Map<String, Object> parameters) {
        return ImmutableMap.of(MAX_STEPS, maxSteps(parameters),
                               ALPHA, alpha(parameters),
                               PRECISION, precision(parameters));
    }

    private static double alpha(Map<String, Object> parameters) {
        if (!parameters.containsKey(ALPHA)) {
            return DEFAULT_ALPHA;
        }
        double alpha = ParameterUtil.parameterDouble(parameters, ALPHA);
        E.checkArgument(alpha > 0 && alpha < 1,
                        "The value of %s must be (0, 1), but got %s",
                        ALPHA, alpha);
        return alpha;
    }
}
