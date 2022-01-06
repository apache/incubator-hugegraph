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

package com.baidu.hugegraph.task;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.computer.driver.util.JsonUtil;

import org.apache.commons.lang.NotImplementedException;

/**
 * Serialize / Deserialize HugeTask that make it could be persisted
 * @author Scorpiour
 * @since 2022-01-05
 */
public final class TaskSerializer {

    private static final List<String> fields
        = Arrays.asList(
            "task_name",
            "task_progress",
            "task_create",
            "task_status",
            "task_retries",
            "id",
            "task_type",
            "task_callable",
            "task_input");

    public static <V> String toJson(HugeTask<V> task) {
        Map<String, Object> map = task.asMap();
        return JsonUtil.toJson(map);
    }

    /**
     * Do not use
     * @param <V>
     * @param task
     * @return
     */
    public static <V> String toYaml(HugeTask<V> task) {
        throw new NotImplementedException();
    }

    public static <V> HugeTask<V> fromJson(String jsonStr) {
        Map<String, Object> map = JsonUtil.fromJson(jsonStr, Map.class);
        String callableStr = String.valueOf(map.get("task_callable"));
        String idStr = String.valueOf(map.get("id"));
        String parentStr = String.valueOf(map.get("parent"));
        String input = String.valueOf(map.get("task_input"));

        Id id = IdGenerator.of(idStr);
        Id parent = IdGenerator.of(parentStr);
        HugeTask<V> task = new HugeTask<>(id, parent, callableStr, input);
        return task;
    }

    
}
