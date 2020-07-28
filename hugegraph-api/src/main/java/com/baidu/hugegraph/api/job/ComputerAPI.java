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

import java.util.Map;

import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;

import org.slf4j.Logger;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.api.API;
import com.baidu.hugegraph.api.filter.StatusFilter.Status;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.core.GraphManager;
import com.baidu.hugegraph.job.ComputerJob;
import com.baidu.hugegraph.job.JobBuilder;
import com.baidu.hugegraph.server.RestServer;
import com.baidu.hugegraph.task.HugeTask;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.JsonUtil;
import com.baidu.hugegraph.util.Log;
import com.codahale.metrics.annotation.Timed;
import com.google.common.collect.ImmutableMap;

@Path("graphs/{graph}/jobs/computer")
@Singleton
public class ComputerAPI extends API {

    private static final Logger LOG = Log.logger(RestServer.class);

    @POST
    @Timed
    @Path("/{name}")
    @Status(Status.CREATED)
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public Map<String, Id> post(@Context GraphManager manager,
                                @PathParam("graph") String graph,
                                @PathParam("name") String computer,
                                Map<String, Object> parameters) {
        LOG.debug("Graph [{}] schedule computer job: {}", graph, parameters);
        E.checkArgument(computer != null && !computer.isEmpty(),
                        "The computer name can't be empty");
        if (parameters == null) {
            parameters = ImmutableMap.of();
        }
        if (!ComputerJob.check(computer, parameters)) {
            throw new NotFoundException("Not found computer: " + computer);
        }

        HugeGraph g = graph(manager, graph);
        Map<String, Object> input = ImmutableMap.of("computer", computer,
                                                    "parameters", parameters);
        JobBuilder<Object> builder = JobBuilder.of(g);
        builder.name("computer:" + computer)
               .input(JsonUtil.toJson(input))
               .job(new ComputerJob());
        HugeTask task = builder.schedule();
        return ImmutableMap.of("task_id", task.id());
    }
}
