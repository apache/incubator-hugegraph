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

import com.baidu.hugegraph.config.CoreOptions;
import com.baidu.hugegraph.job.computer.Computer;
import com.baidu.hugegraph.job.computer.ComputerPool;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.JsonUtil;

public class ComputerJob extends SysJob<Object> {

    public static final String COMPUTER = "computer";

    public static boolean check(String name, Map<String, Object> parameters) {
        Computer computer = ComputerPool.instance().find(name);
        if (computer == null) {
            return false;
        }
        computer.checkParameters(parameters);
        return true;
    }

    public String computerConfig() {
        return this.params().configuration().get(CoreOptions.COMPUTER_CONFIG);
    }

    @Override
    public String type() {
        return COMPUTER;
    }

    @Override
    public Object execute() throws Exception {
        String input = this.task().input();
        E.checkArgumentNotNull(input, "The input can't be null");
        @SuppressWarnings("unchecked")
        Map<String, Object> map = JsonUtil.fromJson(input, Map.class);

        Object value = map.get("computer");
        E.checkArgument(value instanceof String,
                        "Invalid computer name '%s'", value);
        String name = (String) value;

        value = map.get("parameters");
        E.checkArgument(value instanceof Map,
                        "Invalid computer parameters '%s'", value);
        @SuppressWarnings("unchecked")
        Map<String, Object> parameters = (Map<String, Object>) value;

        ComputerPool pool = ComputerPool.instance();
        Computer computer = pool.find(name);
        E.checkArgument(computer != null,
                        "There is no computer method named '%s'", name);
        return computer.call(this, parameters);
    }
}
