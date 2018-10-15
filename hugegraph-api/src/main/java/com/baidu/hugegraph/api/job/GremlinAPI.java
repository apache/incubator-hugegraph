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

import java.util.HashMap;
import java.util.Map;

import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;

import org.slf4j.Logger;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.api.API;
import com.baidu.hugegraph.api.filter.StatusFilter.Status;
import com.baidu.hugegraph.api.schema.Checkable;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.core.GraphManager;
import com.baidu.hugegraph.job.Job;
import com.baidu.hugegraph.job.JobBuilder;
import com.baidu.hugegraph.server.RestServer;
import com.baidu.hugegraph.traversal.optimize.HugeScriptTraversal;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.JsonUtil;
import com.baidu.hugegraph.util.Log;
import com.codahale.metrics.annotation.Timed;

import jersey.repackaged.com.google.common.collect.ImmutableMap;

@Path("graphs/{graph}/jobs/gremlin")
@Singleton
public class GremlinAPI extends API {

    private static final Logger LOG = Log.logger(RestServer.class);

    @POST
    @Timed
    @Status(Status.CREATED)
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public Map<String, Id> post(@Context GraphManager manager,
                                @PathParam("graph") String graph,
                                GremlinRequest request) {
        LOG.debug("Graph [{}] schedule gremlin job: {}", graph, request);
        checkCreatingBody(request);

        HugeGraph g = graph(manager, graph);
        request.aliase(graph, "graph");
        JobBuilder builder = JobBuilder.of(g).name(request.name())
                                       .input(request.toJson())
                                       .job(new GremlinJob());
        return ImmutableMap.of("task_id", builder.schedule());
    }

    public static class GremlinJob extends Job<Object> {

        public static final String TASK_TYPE = "gremlin";

        @Override
        public String type() {
            return TASK_TYPE;
        }

        @Override
        public Object execute() throws Exception {
            GremlinRequest input = GremlinRequest.fromJson(this.task().input());

            HugeScriptTraversal<?, ?> st;
            st = new HugeScriptTraversal<>(this.graph().traversal(),
                                           input.language(), input.gremlin(),
                                           input.bindings(), input.aliases());
            long count = 0;
            long capacity = Query.defaultCapacity(Query.NO_CAPACITY);
            try {
                while (st.hasNext()) {
                    st.next();
                    ++count;
                    Thread.yield();
                }
            } finally {
                Query.defaultCapacity(capacity);
                st.close();
                this.graph().tx().commit();
            }

            Object result = st.result();
            return result != null ? result : count;
        }
    }

    private static class GremlinRequest implements Checkable {

        // See org.apache.tinkerpop.gremlin.server.channel.HttpChannelizer
        public String gremlin;
        public Map<String, Object> bindings = new HashMap<>();
        public String language;
        public Map<String, String> aliases = new HashMap<>();

        public void aliase(String key, String value) {
            this.aliases.put(key, value);
        }

        public Map<String, Object> bindings() {
            return this.bindings;
        }

        public String language() {
            return this.language;
        }

        public String gremlin() {
            return this.gremlin;
        }

        public Map<String, String> aliases() {
            return this.aliases;
        }

        public String name() {
            // Get the first line of script as the name
            return this.gremlin.split("\r\n|\r|\n", 2)[0];
        }

        @Override
        public void checkCreate(boolean isBatch) {
            E.checkArgumentNotNull(this.gremlin,
                                   "The gremlin script can't be null");
            E.checkArgument(this.aliases == null || this.aliases.isEmpty(),
                            "There is no need to pass gremlin aliases");
        }

        public String toJson() {
            return JsonUtil.toJson(this);
        }

        public static GremlinRequest fromJson(String json) {
            return JsonUtil.fromJson(json, GremlinRequest.class);
        }
    }
}
