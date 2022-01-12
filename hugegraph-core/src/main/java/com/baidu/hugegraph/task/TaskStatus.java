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

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;


import com.baidu.hugegraph.type.define.SerialEnum;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

public enum TaskStatus implements SerialEnum {

    UNKNOWN(0, "UNKNOWN"),

    NEW(1, "new"),
    SCHEDULING(2, "scheduling"),
    SCHEDULED(3, "scheduled"),
    QUEUED(4, "queued"),
    RESTORING(5, "restoring"),
    RUNNING(6, "running"),
    SUCCESS(7, "success"),
    CANCELLING(8, "cancelling"),
    CANCELLED(9, "cancelled"),
    FAILED(10, "failed"),
    HANGING(11, "hanging");

    // NOTE: order is important(RESTORING > RUNNING > QUEUED) when restoring
    public static final List<TaskStatus> PENDING_STATUSES = ImmutableList.of(
           TaskStatus.RESTORING, TaskStatus.RUNNING, TaskStatus.QUEUED);

    // Unbreakable status: tasks under this status could not be hung up
    public static final Set<TaskStatus> UNBREAKABLE_STATUSES = ImmutableSet.of(
            TaskStatus.RUNNING, TaskStatus.SUCCESS,
            TaskStatus.CANCELLING, TaskStatus.CANCELLED,
            TaskStatus.FAILED);

    public static final Set<TaskStatus> COMPLETED_STATUSES = ImmutableSet.of(
           TaskStatus.SUCCESS, TaskStatus.CANCELLED, TaskStatus.FAILED);
        
    // Indicates that the task has been occupied by consumer, should not be consumed again
    public static final Set<TaskStatus> OCCUPIED_STATUS = ImmutableSet.of(
        TaskStatus.QUEUED, // task has been taken, wait in the queue
        TaskStatus.RESTORING, // task is restoring
        TaskStatus.RUNNING, // task is executing
        TaskStatus.SUCCESS, // task is executed successfully
        TaskStatus.CANCELLED, // task has been cancelled
        TaskStatus.FAILED); // task failed

    private static final Map<String, TaskStatus> ALL_STATUS_MAP = 
        Stream.of(TaskStatus.values())
        .collect(
            Collectors.toMap(
                status -> status.name,  // name as key
                status -> status,       // status as value
                (prev, next) -> next    // in case of duplicate name
        ));

    private byte status = 0;
    private String name;

    static {
        SerialEnum.register(TaskStatus.class);
    }

    TaskStatus(int status, String name) {
        assert status < 256;
        this.status = (byte) status;
        this.name = name;
    }

    @Override
    public byte code() {
        return this.status;
    }

    public String string() {
        return this.name;
    }

    /**
     * Allowing get TaskStatus by name
     * @param name
     * @return
     */
    public static TaskStatus fromName(String name) {
        TaskStatus status = ALL_STATUS_MAP.getOrDefault(name, UNKNOWN);
        return status;
    }
}
