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

package com.baidu.hugegraph.api.job;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.api.API;
import com.baidu.hugegraph.api.filter.StatusFilter.Status;
import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.backend.page.PageInfo;
import com.baidu.hugegraph.core.GraphManager;
import com.baidu.hugegraph.define.Checkable;
import com.baidu.hugegraph.job.ComputerDisJob;
import com.baidu.hugegraph.job.JobBuilder;
import com.baidu.hugegraph.k8s.K8sDriverProxy;
import com.baidu.hugegraph.server.RestServer;
import com.baidu.hugegraph.task.HugeTask;
import com.baidu.hugegraph.task.TaskScheduler;
import com.baidu.hugegraph.task.TaskStatus;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.JsonUtil;
import com.baidu.hugegraph.util.Log;
import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import org.apache.groovy.util.Maps;
import org.slf4j.Logger;

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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.baidu.hugegraph.backend.query.Query.NO_LIMIT;

@Path("graphs/{graph}/jobs/computerdis")
@Singleton
public class ComputerDisAPI extends API {

    private static final Logger LOG = Log.logger(RestServer.class);

    @POST
    @Timed
    @Status(Status.CREATED)
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public Map<String, Object> post(@Context GraphManager manager,
                                    @PathParam("graph") String graph,
                                    JsonTask jsonTask) {
        LOG.debug("Schedule computer dis job: {}", jsonTask);
        E.checkArgument(K8sDriverProxy.isK8sApiEnabled() == true,
                        "The k8s api is not enable.");
        checkCreatingBody(jsonTask);

        // username is "" means generate token from current context
        String token = "";
        if (manager.isAuthRequired()) {
            token = manager.authManager().createToken("");
        }

        Map<String, Object> input = ImmutableMap.of(
                            "graph", graph,
                            "algorithm", jsonTask.algorithm,
                            "params", jsonTask.params,
                            "worker", jsonTask.worker,
                            "token", token);
        HugeGraph g = graph(manager, graph);
        JobBuilder<Object> builder = JobBuilder.of(g);
        builder.name("computer-dis:" + jsonTask.algorithm)
               .input(JsonUtil.toJson(input))
               .job(new ComputerDisJob());
        HugeTask<Object> task = builder.schedule();
        return ImmutableMap.of("task_id", task.id());
    }

    @DELETE
    @Timed
    @Path("/{id}")
    @Status(Status.OK)
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public Map<String, Object> delete(@Context GraphManager manager,
                                      @PathParam("graph") String graph,
                                      @PathParam("id") long id) {
        LOG.debug("Graph [{}] delete computer job: {}", graph, id);
        E.checkArgument(K8sDriverProxy.isK8sApiEnabled() == true,
                        "The k8s api is not enable.");

        TaskScheduler scheduler = graph(manager, graph).taskScheduler();
        HugeTask<?> task = scheduler.task(IdGenerator.of(id));
        E.checkArgument(ComputerDisJob.COMPUTER_DIS.equals(task.type()),
                        "The task is not computer-dis task.");

        scheduler.delete(IdGenerator.of(id));
        return ImmutableMap.of("task_id", id, "message", "success");
    }

    @PUT
    @Timed
    @Path("/{id}")
    @Status(Status.ACCEPTED)
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public Map<String, Object> cancel(@Context GraphManager manager,
                                      @PathParam("graph") String graph,
                                      @PathParam("id") long id) {
        LOG.debug("Graph [{}] cancel computer job: {}", graph, id);
        E.checkArgument(K8sDriverProxy.isK8sApiEnabled() == true,
                        "The k8s api is not enable.");

        TaskScheduler scheduler = graph(manager, graph).taskScheduler();
        HugeTask<?> task = scheduler.task(IdGenerator.of(id));
        E.checkArgument(ComputerDisJob.COMPUTER_DIS.equals(task.type()),
                        "The task is not computer-dis task.");

        if (!task.completed() && !task.cancelling()) {
            scheduler.cancel(task);
            if (task.cancelling()) {
                return task.asMap();
            }
        }

        assert task.completed() || task.cancelling();
        return ImmutableMap.of("task_id", id);
    }

    @GET
    @Timed
    @Path("/{id}")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public Map<String, Object> get(@Context GraphManager manager,
                                   @PathParam("graph") String graph,
                                   @PathParam("id") long id) {
        LOG.debug("Graph [{}] get task info", graph);
        E.checkArgument(K8sDriverProxy.isK8sApiEnabled() == true,
                        "The k8s api is not enable.");
        TaskScheduler scheduler = graph(manager, graph).taskScheduler();
        HugeTask<Object> task = scheduler.task(IdGenerator.of(id));
        E.checkArgument(ComputerDisJob.COMPUTER_DIS.equals(task.type()),
                        "The task is not computer-dis task.");
        return task.asMap();
    }

    @GET
    @Timed
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public Map<String, Object> list(@Context GraphManager manager,
                                    @PathParam("graph") String graph,
                                    @QueryParam("limit")
                                    @DefaultValue("100") long limit,
                                    @QueryParam("page") String page) {
        LOG.debug("Graph [{}] get task list", graph);
        E.checkArgument(K8sDriverProxy.isK8sApiEnabled() == true,
                        "The k8s api is not enable.");

        TaskScheduler scheduler = graph(manager, graph).taskScheduler();
        Iterator<HugeTask<Object>> iter  = scheduler.tasks(null,
                                                           NO_LIMIT, page);
        List<Object> tasks = new ArrayList<>();
        while (iter.hasNext()) {
            HugeTask<Object> task = iter.next();
            if (ComputerDisJob.COMPUTER_DIS.equals(task.type())) {
                tasks.add(task.asMap(false));
            }
        }
        if (limit != NO_LIMIT && tasks.size() > limit) {
            tasks = tasks.subList(0, (int) limit);
        }

        if (page == null) {
            return Maps.of("tasks", tasks);
        } else {
            return Maps.of("tasks", tasks, "page", PageInfo.pageInfo(iter));
        }
    }

    private static class JsonTask implements Checkable {

        @JsonProperty("algorithm")
        public String algorithm;
        @JsonProperty("worker")
        public int worker;
        @JsonProperty("params")
        public Map<String, Object> params;

        @Override
        public void checkCreate(boolean isBatch) {
            E.checkArgument(this.algorithm != null &&
                            K8sDriverProxy.isValidAlgorithm(this.algorithm),
                            "The algorithm is not existed.");
            E.checkArgument(this.worker >= 1 &&
                            this.worker <= 100,
                            "The worker should be in [1, 100].");
        }

        @Override
        public void checkUpdate() {}
    }

    private static TaskStatus parseStatus(String status) {
        try {
            return TaskStatus.valueOf(status);
        } catch (Exception e) {
            throw new IllegalArgumentException(String.format(
                      "Status value must be in %s, but got '%s'",
                      Arrays.asList(TaskStatus.values()), status));
        }
    }
}
