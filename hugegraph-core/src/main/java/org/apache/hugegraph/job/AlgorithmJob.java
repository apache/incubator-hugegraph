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

package org.apache.hugegraph.job;

import java.util.Map;

import org.apache.hugegraph.job.algorithm.Algorithm;
import org.apache.hugegraph.job.algorithm.AlgorithmPool;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.JsonUtil;

public class AlgorithmJob extends UserJob<Object> {

    public static final String TASK_TYPE = "algorithm";

    public static boolean check(String name, Map<String, Object> parameters) {
        Algorithm algorithm = AlgorithmPool.instance().find(name);
        if (algorithm == null) {
            return false;
        }
        algorithm.checkParameters(parameters);
        return true;
    }

    @Override
    public String type() {
        return TASK_TYPE;
    }

    @Override
    public Object execute() throws Exception {
        String input = this.task().input();
        E.checkArgumentNotNull(input, "The input can't be null");
        @SuppressWarnings("unchecked")
        Map<String, Object> map = JsonUtil.fromJson(input, Map.class);

        Object value = map.get("algorithm");
        E.checkArgument(value instanceof String,
                        "Invalid algorithm name '%s'", value);
        String name = (String) value;

        value = map.get("parameters");
        E.checkArgument(value instanceof Map,
                        "Invalid algorithm parameters '%s'", value);
        @SuppressWarnings("unchecked")
        Map<String, Object> parameters = (Map<String, Object>) value;

        AlgorithmPool pool = AlgorithmPool.instance();
        Algorithm algorithm = pool.find(name);
        E.checkArgument(algorithm != null,
                        "There is no algorithm named '%s'", name);
        return algorithm.call(this, parameters);
    }
}
