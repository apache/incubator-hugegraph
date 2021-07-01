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

package com.baidu.hugegraph.api.auth;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;

import org.slf4j.Logger;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.api.API;
import com.baidu.hugegraph.api.filter.StatusFilter;
import com.baidu.hugegraph.auth.AuthManager;
import com.baidu.hugegraph.auth.HugeProject;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.core.GraphManager;
import com.baidu.hugegraph.define.Checkable;
import com.baidu.hugegraph.exception.NotFoundException;
import com.baidu.hugegraph.server.RestServer;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;
import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Strings;

@Path("graphs/{graph}/auth/projects")
@Singleton
public class ProjectAPI extends API {

    private static final Logger LOG = Log.logger(RestServer.class);
    private static final String ACTION_ADD_GRAPH = "add_graph";
    private static final String ACTION_DELETE_GRAPH = "delete_graph";

    @POST
    @Timed
    @StatusFilter.Status(StatusFilter.Status.CREATED)
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String create(@Context GraphManager manager,
                         @PathParam("graph") String graph,
                         JsonProject jsonProject) {
        LOG.debug("Graph [{}] create project: {}", graph, jsonProject);
        checkCreatingBody(jsonProject);

        HugeGraph g = graph(manager, graph);
        HugeProject project = jsonProject.build();
        Id projectId = manager.authManager().createProject(project);
        /*
         * Some fields of project(like admin_group) can only be known after
         * created
         */
        project = manager.authManager().getProject(projectId);
        return manager.serializer(g).writeAuthElement(project);
    }

    @PUT
    @Timed
    @Path("{id}")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String update(@Context GraphManager manager,
                         @PathParam("graph") String graph,
                         @PathParam("id") String id,
                         @QueryParam("action") String action,
                         JsonProject jsonProject) {
        LOG.debug("Graph [{}] update {} project: {}", graph, action,
                  jsonProject);

        HugeGraph g = graph(manager, graph);
        HugeProject project;
        Id projectId = UserAPI.parseId(id);
        AuthManager authManager = manager.authManager();
        try {
            project = authManager.getProject(projectId);
        } catch (NotFoundException e) {
            throw new IllegalArgumentException("Invalid project id: " + id);
        }
        project = jsonProject.build(action, project);
        if (this.isAddGraph(action)) {
            authManager.updateProjectAddGraph(projectId, jsonProject.graph);
        } else if (this.isDeleteGraph(action)) {
            authManager.updateProjectRemoveGraph(projectId, jsonProject.graph);
        } else {
            authManager.updateProject(project);
        }
        return manager.serializer(g).writeAuthElement(project);
    }

    @GET
    @Timed
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String list(@Context GraphManager manager,
                       @PathParam("graph") String graph,
                       @QueryParam("limit") @DefaultValue("100") long limit) {
        LOG.debug("Graph [{}] list project", graph);

        HugeGraph g = graph(manager, graph);
        List<HugeProject> projects = manager.authManager()
                                            .listAllProject(limit);
        return manager.serializer(g).writeAuthElements("projects", projects);
    }

    @GET
    @Timed
    @Path("{id}")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String get(@Context GraphManager manager,
                      @PathParam("graph") String graph,
                      @PathParam("id") String id) {
        LOG.debug("Graph [{}] get project: {}", graph, id);

        HugeGraph g = graph(manager, graph);
        HugeProject project;
        try {
            project = manager.authManager().getProject(UserAPI.parseId(id));
        } catch (NotFoundException e) {
            throw new IllegalArgumentException("Invalid project id: " + id);
        }
        return manager.serializer(g).writeAuthElement(project);
    }

    @DELETE
    @Timed
    @Path("{id}")
    @Consumes(APPLICATION_JSON)
    public void delete(@Context GraphManager manager,
                       @PathParam("graph") String graph,
                       @PathParam("id") String id) {
        LOG.debug("Graph [{}] delete project: {}", graph, id);

        @SuppressWarnings("unused") // just check if the graph exists
        HugeGraph g = graph(manager, graph);
        try {
            manager.authManager().deleteProject(UserAPI.parseId(id));
        } catch (NotFoundException e) {
            throw new IllegalArgumentException("Invalid project id: " + id);
        }
    }

    public static boolean isAddGraph(String action) {
        return ACTION_ADD_GRAPH.equals(action);
    }

    public static boolean isDeleteGraph(String action) {
        return ACTION_DELETE_GRAPH.equals(action);
    }

    private static class JsonProject implements Checkable {

        @JsonProperty("project_description")
        private String description;
        @JsonProperty("project_name")
        private String name;
        @JsonProperty("project_graph")
        private String graph;

        public HugeProject build(String action, HugeProject project) {
            if (ProjectAPI.isAddGraph(action)) {
                this.checkGraph();
                Set<String> graphs = new HashSet<>(project.graphs());
                graphs.add(this.graph);
                project.graphs(graphs);
            } else if (ProjectAPI.isDeleteGraph(action)) {
                this.checkGraph();
                Set<String> graphs = new HashSet<>(project.graphs());
                graphs.remove(this.graph);
                project.graphs(graphs);
            } else {
                this.checkDescription();
                project.description(this.description);
            }
            return project;
        }

        private void checkGraph() {
            E.checkArgument(!Strings.isNullOrEmpty(this.graph),
                            "The graph of project can't be empty");
        }

        private void checkDescription() {
            E.checkArgumentNotNull(this.description,
                                   "The description of project " +
                                   "can't be null");
        }

        public HugeProject build() {
            HugeProject project = new HugeProject(this.name, this.description);
            return project;
        }

        @Override
        public void checkCreate(boolean isBatch) {
            E.checkArgumentNotNull(this.name,
                                   "The name of project can't be null");
        }

        @Override
        public void checkUpdate() {
            E.checkArgumentNotNull(this.description,
                                   "The description of project " +
                                   "can't be null");
        }
    }
}
