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
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.inject.Singleton;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;

import org.slf4j.Logger;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.api.API;
import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.core.GraphManager;
import com.baidu.hugegraph.server.RestServer;
import com.baidu.hugegraph.task.HugeTask;
import com.baidu.hugegraph.task.HugeTaskScheduler;
import com.baidu.hugegraph.task.Status;
import com.baidu.hugegraph.util.Log;
import com.codahale.metrics.annotation.Timed;
import com.google.common.collect.ImmutableMap;

@Path("graphs/{graph}/tasks")
@Singleton
public class TaskAPI extends API {

    private static final Logger LOG = Log.logger(RestServer.class);

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

        HugeGraph g = graph(manager, graph);
        HugeTaskScheduler scheduler = g.taskScheduler();

        Iterator<HugeTask<Object>> itor;
        if (status == null) {
            itor = scheduler.findAllTask(limit);
        } else {
            itor = scheduler.findTask(parseStatus(status), limit);
        }

        List<Object> tasks = new ArrayList<>();
        while (itor.hasNext()) {
            tasks.add(itor.next().asMap());
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

        HugeGraph g = graph(manager, graph);
        HugeTaskScheduler scheduler = g.taskScheduler();
        return scheduler.task(IdGenerator.of(id)).asMap();
    }

    @DELETE
    @Timed
    @Path("{id}")
    public void delete(@Context GraphManager manager,
                       @PathParam("graph") String graph,
                       @PathParam("id") long id) {
        LOG.debug("Graph [{}] delete task: {}", graph, id);

        HugeGraph g = graph(manager, graph);
        HugeTaskScheduler scheduler = g.taskScheduler();

        scheduler.deleteTask(IdGenerator.of(id));
    }

    private static Status parseStatus(String status) {
        try {
            return Status.valueOf(status);
        } catch (Exception e) {
            throw new IllegalArgumentException(String.format(
                      "Status value must be in [UNKNOWN, NEW, QUEUED, " +
                      "RESTORING, RUNNING, SUCCESS, CANCELLED, FAILED], " +
                      "but got '%s'", status));
        }
    }
}