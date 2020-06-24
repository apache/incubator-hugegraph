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

import jersey.repackaged.com.google.common.collect.ImmutableMap;

public class TaskApiTest extends BaseApiTest {

    private static String path = "/graphs/hugegraph/tasks/";

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

        waitTaskSuccess(taskId);
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
    public void testCancel() {
        int taskId = this.gremlinJob();

        sleepAWhile();
        Map<String, Object> params = ImmutableMap.of("action", "cancel");
        Response r = client().put(path, String.valueOf(taskId), "", params);
        String content = r.readEntity(String.class);
        Assert.assertTrue(content,
                          r.getStatus() == 202 || r.getStatus() == 400);
        if (r.getStatus() == 202) {
            String status = assertJsonContains(content, "task_status");
            Assert.assertEquals("cancelling", status);
        } else {
            assert r.getStatus() == 400;
            String error = String.format(
                           "Can't cancel task '%s' which is completed", taskId);
            Assert.assertContains(error, content);

            r = client().get(path, String.valueOf(taskId));
            content = assertResponseStatus(200, r);
            String status = assertJsonContains(content, "task_status");
            Assert.assertEquals("success", status);
        }
    }

    @Test
    public void testDelete() {
        int taskId = this.rebuild();

        waitTaskSuccess(taskId);
        Response r = client().delete(path, String.valueOf(taskId));
        assertResponseStatus(204, r);
    }

    private int rebuild() {
        String rebuildPath = "/graphs/hugegraph/jobs/rebuild/indexlabels";
        String personByCity = "personByCity";
        Map<String, Object> params = ImmutableMap.of();
        Response r = client().put(rebuildPath, personByCity, "",  params);
        String content = assertResponseStatus(202, r);
        return assertJsonContains(content, "task_id");
    }

    private int gremlinJob() {
        String body = "{"
                + "\"gremlin\":\"Thread.sleep(1000L)\","
                + "\"bindings\":{},"
                + "\"language\":\"gremlin-groovy\","
                + "\"aliases\":{}}";
        String path = "/graphs/hugegraph/jobs/gremlin";
        String content = assertResponseStatus(201, client().post(path, body));
        return assertJsonContains(content, "task_id");
    }

    private void sleepAWhile() {
        try {
            Thread.sleep(200L);
        } catch (InterruptedException e) {
            // ignore
        }
    }
}
