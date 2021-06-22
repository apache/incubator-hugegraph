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

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Response;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Strings;

public class ProjectApiTest extends BaseApiTest {

    private final static String path = "graphs/hugegraph/auth/projects";

    @Before
    public void setup() {
        BaseApiTest.truncate();
    }

    @Test
    public void testCreate() {
        createProject("test_project", "this is a good project");
    }

    private String createProject(String name, String desc) {
        String reqBody = String.format("{\"name\": \"%s\",\"desc\": \"%s\"}",
                                       name, desc);
        Response resp = client().post(path, reqBody);
        String respBody = assertResponseStatus(201, resp);
        String projectName = assertJsonContains(respBody, "project_name");
        Assert.assertEquals(name, projectName);
        if (!Strings.isNullOrEmpty(desc)) {
            String projectDesc = assertJsonContains(respBody,
                                                    "project_description");
            Assert.assertEquals(desc, projectDesc);
        }
        Assert.assertFalse(Strings.isNullOrEmpty(assertJsonContains(respBody,
                                                                    "project_target")));
        Assert.assertFalse(Strings.isNullOrEmpty(assertJsonContains(respBody,
                                                                    "project_admin_group")));
        Assert.assertFalse(Strings.isNullOrEmpty(assertJsonContains(respBody,
                                                                    "project_op_group")));
        return respBody;
    }

    @Test
    public void testDelete() {
        String respBody = createProject("test_project1",
                                        "this is a good project");
        String id = assertJsonContains(respBody, "id");
        Response resp = client().target()
                                .path(path)
                                .path(id)
                                .request()
                                .delete();
        assertResponseStatus(204, resp);
    }

    @Test
    public void testGet() {
        String respBody = createProject("test_project",
                                        "this is a good project");
        String projectId = assertJsonContains(respBody, "id");
        String respBody2 = getProject(projectId);
        Assert.assertEquals(respBody, respBody2);
    }

    @Test
    public void testList() {
        createProject("test_project", null);
        createProject("test_project2", null);
        Response resp = client().get(path);
        String respBody = assertResponseStatus(200, resp);
        List<Map> projects = readList(respBody, "projects", Map.class);
        Assert.assertNotNull(projects);
        Assert.assertEquals(2, projects.size());
    }

    @Test
    public void testUpdate() {
        String reqBody = "{\"desc\": \"update desc\"}";
        Response resp = client().target()
                                .path(path)
                                .path("no_exist_id")
                                .request()
                                .put(Entity.json(reqBody));
        assertResponseStatus(400, resp);

        String projectId = assertJsonContains(createProject("test_project",
                                                            "desc"),
                                              "id");
        resp = client().target()
                       .path(path)
                       .path(projectId)
                       .request()
                       .put(Entity.json(reqBody));
        String respBody = assertResponseStatus(200, resp);
        String desc = assertJsonContains(respBody, "project_description");
        Assert.assertEquals("update desc", desc);
    }

    @Test
    public void testUpdateAddGraph() {
        String projectId = makeProjectWithGraph("project_test", "graph_test");
        String respBody = getProject(projectId);
        assertJsonContains(respBody, "project_graphs");
        makeGraph(projectId, "graph_test2");
        respBody = getProject(projectId);
        List<String> graphs = assertJsonContains(respBody, "project_graphs");
        Assert.assertEquals(2, graphs.size());
        Assert.assertTrue(graphs.contains("graph_test"));
        Assert.assertTrue(graphs.contains("graph_test2"));
    }

    @Test
    public void testUpdateRemoveGraph() {
        String projectId = makeProjectWithGraph("project_test", "graph_test");
        deleteGraph(projectId, "graph_test");

        String respBody = getProject(projectId);
        Assert.assertFalse(respBody.contains("project_graphs"));

        makeGraph(projectId, "graph_test1");
        makeGraph(projectId, "graph_test2");
        deleteGraph(projectId, "graph_test2");
        respBody = getProject(projectId);
        List<String> graphs = assertJsonContains(respBody, "project_graphs");
        Assert.assertEquals(1, graphs.size());
        Assert.assertTrue(graphs.contains("graph_test1"));
    }

    private String makeProjectWithGraph(String projectName,
                                       String graph) {
        String projectId = assertJsonContains(createProject(projectName,
                                                            null), "id");
        makeGraph(projectId, graph);
        return projectId;
    }

    private void makeGraph(String projectId, String graph) {
        String reqBody = String.format("{\"graph\":\"%s\"}", graph);
        Response resp = client().target()
                                .path(path)
                                .path(projectId)
                                .queryParam("action", "add_graph")
                                .request()
                                .put(Entity.json(reqBody));
        assertResponseStatus(200, resp);
    }

    private void deleteGraph(String projectId, String graph) {
        String reqBody = String.format("{\"graph\":\"%s\"}", graph);
        Response resp = client().target()
                                .path(path)
                                .path(projectId)
                                .queryParam("action", "delete_graph")
                                .request()
                                .put(Entity.json(reqBody));
        assertResponseStatus(200, resp);
    }

    private String getProject(String projectId) {
        Response resp = client().target()
                                .path(path)
                                .path(projectId)
                                .request()
                                .get();
        String respBody = assertResponseStatus(200, resp);
        return respBody;
    }
}
