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

package com.baidu.hugegraph.api;

import java.util.List;
import java.util.Map;

import javax.ws.rs.core.Response;

import org.junit.Before;
import org.junit.Test;

import com.baidu.hugegraph.testutil.Assert;

import jersey.repackaged.com.google.common.collect.ImmutableList;
import jersey.repackaged.com.google.common.collect.ImmutableMap;

public class TaskApiTest extends BaseApiTest {

    private static String path = "/graphs/hugegraph/tasks/";
    private static String rebuildPath =
            "/graphs/hugegraph/jobs/rebuild/indexlabels/personByCity";
    private static String personByCity = "personByCity";
    private static Map<String, Object> personByCityIL = ImmutableMap.of(
            "name", "personByCity",
            "base_type", "VERTEX_LABEL",
            "base_value", "person",
            "index_type", "SECONDARY",
            "fields", ImmutableList.of("city"));

    @Before
    public void prepareSchema() {
        BaseApiTest.initPropertyKey();
        BaseApiTest.initVertexLabel();
        BaseApiTest.initIndexLabel();
    }

    @Test
    public void testList() {
        int taskId = this.rebuild();

        Response r = client().get(path, ImmutableMap.of("limit", -1));
        String content = assertResponseStatus(200, r);
        List<Map<?, ?>> tasks = assertJsonContains(content, "tasks");
        assertArrayContains(tasks, "id", taskId);

        this.waitTaskSuccess(taskId);
        r = client().get(path, ImmutableMap.of("status", "RUNNING"));
        content = assertResponseStatus(200, r);
        tasks = assertJsonContains(content, "tasks");
        Assert.assertTrue(tasks.isEmpty());
    }

    @Test
    public void testGet() {
        int taskId = this.rebuild();

        Response r = client().get(path, String.valueOf(taskId));
        String content = assertResponseStatus(200, r);
        assertJsonContains(content, "id");
    }

    @Test
    public void testDelete() {
        int taskId = this.rebuild();

        this.waitTaskSuccess(taskId);
        Response r = client().delete(path, String.valueOf(taskId));
        assertResponseStatus(204, r);
    }

    private int rebuild() {
        Response r = client().put(rebuildPath, personByCity, personByCityIL);
        String content = assertResponseStatus(202, r);
        return assertJsonContains(content, "task_id");
    }

    private void waitTaskSuccess(int task) {
        String status;
        do {
            Response r = client().get(path, String.valueOf(task));
            String content = assertResponseStatus(200, r);
            status = assertJsonContains(content, "task_status");
        } while (!status.equals("success"));
    }
}
