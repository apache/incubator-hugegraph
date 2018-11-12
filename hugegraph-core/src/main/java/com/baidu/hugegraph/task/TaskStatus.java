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
import java.util.Set;

import com.baidu.hugegraph.type.define.SerialEnum;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

public enum TaskStatus implements SerialEnum {

    UNKNOWN(0, "UNKNOWN"),

    NEW(1, "new"),
    QUEUED(2, "queued"),
    RESTORING(3, "restoring"),
    RUNNING(4, "running"),
    SUCCESS(5, "success"),
    CANCELLED(6, "cancelled"),
    FAILED(7, "failed");

    // NOTE: order is important(RESTORING > RUNNING > QUEUED) when restoring
    public static final List<TaskStatus> PENDING_STATUSES = ImmutableList.of(
           TaskStatus.RESTORING, TaskStatus.RUNNING, TaskStatus.QUEUED);

    public static final Set<TaskStatus> COMPLETED_STATUSES = ImmutableSet.of(
           TaskStatus.SUCCESS, TaskStatus.CANCELLED, TaskStatus.FAILED);

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
}
