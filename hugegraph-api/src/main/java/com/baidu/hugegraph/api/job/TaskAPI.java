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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.inject.Singleton;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.NotSupportedException;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;

import org.slf4j.Logger;

import com.baidu.hugegraph.api.API;
import com.baidu.hugegraph.api.filter.StatusFilter.Status;
import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.core.GraphManager;
import com.baidu.hugegraph.server.RestServer;
import com.baidu.hugegraph.task.HugeTask;
import com.baidu.hugegraph.task.TaskScheduler;
import com.baidu.hugegraph.task.TaskStatus;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;
import com.codahale.metrics.annotation.Timed;
import com.google.common.collect.ImmutableMap;

@Path("graphs/{graph}/tasks")
@Singleton
public class TaskAPI extends API {

    private static final Logger LOG = Log.logger(RestServer.class);

    public static final String ACTION_CANCEL = "cancel";

    @GET
    @Timed
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public Map<String, List<Object>> list(@Context GraphManager manager,
                                          @PathParam("graph") String graph,
                                          @QueryParam("status") String status,
                                          @QueryParam("limit")
                                          @DefaultValue("100") long limit) {
        LOG.debug("Graph [{}] list tasks with status {}, limit {}",
                  graph, status, limit);

        TaskScheduler scheduler = graph(manager, graph).taskScheduler();

        Iterator<HugeTask<Object>> itor;
        if (status == null) {
            itor = scheduler.findAllTask(limit);
        } else {
            itor = scheduler.findTask(parseStatus(status), limit);
        }

        List<Object> tasks = new ArrayList<>();
        while (itor.hasNext()) {
            tasks.add(itor.next().asMap(false));
        }
        return ImmutableMap.of("tasks", tasks);
    }

    @GET
    @Timed
    @Path("{id}")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public Map<String, Object> get(@Context GraphManager manager,
                                   @PathParam("graph") String graph,
                                   @PathParam("id") long id) {
        LOG.debug("Graph [{}] get task: {}", graph, id);

        TaskScheduler scheduler = graph(manager, graph).taskScheduler();
        return scheduler.task(IdGenerator.of(id)).asMap();
    }

    @DELETE
    @Timed
    @Path("{id}")
    public void delete(@Context GraphManager manager,
                       @PathParam("graph") String graph,
                       @PathParam("id") long id) {
        LOG.debug("Graph [{}] delete task: {}", graph, id);

        TaskScheduler scheduler = graph(manager, graph).taskScheduler();
        HugeTask<?> task = scheduler.deleteTask(IdGenerator.of(id));
        E.checkArgument(task != null, "There is no task with id '%s'", id);
    }

    @PUT
    @Timed
    @Path("{id}")
    @Status(Status.ACCEPTED)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public Map<String, Object> update(@Context GraphManager manager,
                                      @PathParam("graph") String graph,
                                      @PathParam("id") long id,
                                      @QueryParam("action") String action) {
        LOG.debug("Graph [{}] cancel task: {}", graph, id);

        if (!ACTION_CANCEL.equals(action)) {
            throw new NotSupportedException(String.format(
                      "Not support action '%s'", action));
        }

        TaskScheduler scheduler = graph(manager, graph).taskScheduler();
        HugeTask<?> task = scheduler.task(IdGenerator.of(id));
        if (!task.completed()) {
            scheduler.cancel(task);
        } else {
            throw new BadRequestException(String.format(
                      "Can't cancel task '%s' which is completed", id));
        }
        return ImmutableMap.of("cancelled", task.isCancelled());
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