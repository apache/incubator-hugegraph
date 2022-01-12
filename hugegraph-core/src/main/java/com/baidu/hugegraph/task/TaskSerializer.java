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

import java.util.Date;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.computer.driver.util.JsonUtil;

import org.apache.commons.lang.NotImplementedException;

import jersey.repackaged.com.google.common.collect.ImmutableSet;

/**
 * Serialize / Deserialize HugeTask that make it could be persisted
 * @author Scorpiour
 * @since 2022-01-05
 */
public final class TaskSerializer {

    /**
     * Meta info of serialized task
     */
    private enum TaskField {
        ID("id"),
        TASK_NAME("task_name"),
        TASK_CREATE("task_create"),
        TASK_RETIRES("task_retries"),
        TASK_TYPE("task_type"),
        TASK_CALLABLE("task_callable"),
        TASK_INPUT("task_input"),
        TASK_PRIORITY("task_priority"),

        ;

        public static final ImmutableSet<String> FIELD_SET = 
            ImmutableSet.copyOf(
                Stream.of(
                    TaskField.values()).map(TaskField::getValue).collect(Collectors.toSet()));
        private final String value;
        private TaskField(String value) {
            this.value = value;
        }
        public String getValue() {
            return this.value;
        }
    }

    public static <V> String toJson(HugeTask<V> task) {
        Map<String, Object> map = task.asMap();
        map = map
                .entrySet()
                .stream()
                .filter(entry -> 
                    TaskField.FIELD_SET
                        .contains(entry.getKey()))
                .collect(
                    Collectors.toMap(
                        Map.Entry<String, Object>::getKey,
                        Map.Entry<String, Object>::getValue));
        
        return JsonUtil.toJson(map);
    }

    /**
     * Do not use it
     * @param <V>
     * @param task
     * @return
     */
    public static <V> String toYaml(HugeTask<V> task) {
        throw new NotImplementedException();
    }

    /**
     * Deserialize task from json
     * @param <V>
     * @param jsonStr
     * @return
     */
    @SuppressWarnings("unchecked")
    public static <V> HugeTask<V> fromJson(String jsonStr) {
        Map<String, Object> map = JsonUtil.fromJson(jsonStr, Map.class);
        String callableStr
            = String.valueOf(
                map.get(
                    TaskField.TASK_CALLABLE.getValue()));
        Integer numId
            = Integer.valueOf(
                String.valueOf(
                    map.get(
                        TaskField.ID.getValue())));
        String input
            = String.valueOf(
                map.get(
                    TaskField.TASK_INPUT.getValue()));
        String typeStr
            = String.valueOf(
                map.get(
                    TaskField.TASK_TYPE.getValue()));
        Date createdAt
            = new Date(
                Long.valueOf(
                    String.valueOf(
                        map.get(
                            TaskField.TASK_CREATE.getValue()))));
        String name
            = String.valueOf(
                map.get(
                    TaskField.TASK_NAME.getValue()));

        Id id = IdGenerator.of(numId);
        HugeTask<V> task = new HugeTask<>(id, null, callableStr, input);

        /**
         * Fill rest of the properties
         * Be ware of progress and status should not be modified here
         * They are updated by scheduler / executor
         */
        task.name(name);
        task.type(typeStr);
        task.createTime(createdAt);

        // Recursive dependency here, but should be maintain due to compatible
        task.callable().task(task);

        return task;
    }

    
}
