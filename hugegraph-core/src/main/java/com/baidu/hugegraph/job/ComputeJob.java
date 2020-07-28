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

package com.baidu.hugegraph.job;

import java.util.Map;

import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.job.compute.Compute;
import com.baidu.hugegraph.job.compute.ComputePool;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.JsonUtil;

public class ComputeJob extends SysJob<Object> {

    public static final String COMPUTE = "compute";

    public static boolean check(String name, Map<String, Object> parameters) {
        Compute algorithm = ComputePool.instance().find(name);
        if (algorithm == null) {
            return false;
        }
        algorithm.checkParameters(parameters);
        return true;
    }

    public HugeConfig config() {
        return this.params().configuration();
    }

    @Override
    public String type() {
        return COMPUTE;
    }

    @Override
    public Object execute() throws Exception {
        String input = this.task().input();
        E.checkArgumentNotNull(input, "The input can't be null");
        @SuppressWarnings("unchecked")
        Map<String, Object> map = JsonUtil.fromJson(input, Map.class);

        Object value = map.get("compute");
        E.checkArgument(value instanceof String,
                        "Invalid compute name '%s'", value);
        String name = (String) value;

        value = map.get("parameters");
        E.checkArgument(value instanceof Map,
                        "Invalid compute parameters '%s'", value);
        @SuppressWarnings("unchecked")
        Map<String, Object> parameters = (Map<String, Object>) value;

        ComputePool pool = ComputePool.instance();
        Compute compute = pool.find(name);
        E.checkArgument(compute != null,
                        "There is no compute method named '%s'", name);
        compute.checkParameters(parameters);
        return compute.call(this, parameters);
    }
}
