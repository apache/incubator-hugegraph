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

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.util.HashMap;
import java.util.Map;

import javax.annotation.security.RolesAllowed;
import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;

import org.slf4j.Logger;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.api.API;
import com.baidu.hugegraph.api.filter.StatusFilter.Status;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.core.GraphManager;
import com.baidu.hugegraph.define.Checkable;
import com.baidu.hugegraph.job.GremlinJob;
import com.baidu.hugegraph.job.JobBuilder;
import com.baidu.hugegraph.metrics.MetricsUtil;
import com.baidu.hugegraph.server.RestServer;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.JsonUtil;
import com.baidu.hugegraph.util.Log;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

@Path("graphs/{graph}/jobs/gremlin")
@Singleton
public class GremlinAPI extends API {

    private static final Logger LOG = Log.logger(RestServer.class);

    private static final int MAX_NAME_LENGTH = 256;

    private static final Histogram gremlinJobInputHistogram =
            MetricsUtil.registerHistogram(GremlinAPI.class, "gremlin-input");

    @POST
    @Timed
    @Status(Status.CREATED)
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"admin", "$owner=$graph $action=gremlin_execute"})
    public Map<String, Id> post(@Context GraphManager manager,
                                @PathParam("graph") String graph,
                                GremlinRequest request) {
        LOG.debug("Graph [{}] schedule gremlin job: {}", graph, request);
        checkCreatingBody(request);
        gremlinJobInputHistogram.update(request.gremlin.length());

        HugeGraph g = graph(manager, graph);
        request.aliase(graph, "graph");
        JobBuilder<Object> builder = JobBuilder.of(g);
        builder.name(request.name())
               .input(request.toJson())
               .job(new GremlinJob());
        return ImmutableMap.of("task_id", builder.schedule().id());
    }

    public static class GremlinRequest implements Checkable {

        // See org.apache.tinkerpop.gremlin.server.channel.HttpChannelizer
        @JsonProperty
        private String gremlin;
        @JsonProperty
        private Map<String, Object> bindings = new HashMap<>();
        @JsonProperty
        private String language = "gremlin-groovy";
        @JsonProperty
        private Map<String, String> aliases = new HashMap<>();

        public String gremlin() {
            return this.gremlin;
        }

        public void gremlin(String gremlin) {
            this.gremlin = gremlin;
        }

        public Map<String, Object> bindings() {
            return this.bindings;
        }

        public void bindings(Map<String, Object> bindings) {
            this.bindings = bindings;
        }

        public void binding(String name, Object value) {
            this.bindings.put(name, value);
        }

        public String language() {
            return this.language;
        }

        public void language(String language) {
            this.language = language;
        }

        public Map<String, String> aliases() {
            return this.aliases;
        }

        public void aliases(Map<String, String> aliases) {
            this.aliases = aliases;
        }

        public void aliase(String key, String value) {
            this.aliases.put(key, value);
        }

        public String name() {
            // Get the first line of script as the name
            String firstLine = this.gremlin.split("\r\n|\r|\n", 2)[0];
            final Charset charset = Charset.forName(CHARSET);
            final byte[] bytes = firstLine.getBytes(charset);
            if (bytes.length <= MAX_NAME_LENGTH) {
                return firstLine;
            }

            /*
             * Reference https://stackoverflow.com/questions/3576754/truncating-strings-by-bytes
             */
            CharsetDecoder decoder = charset.newDecoder();
            decoder.onMalformedInput(CodingErrorAction.IGNORE);
            decoder.reset();

            ByteBuffer buffer = ByteBuffer.wrap(bytes, 0, MAX_NAME_LENGTH);
            try {
                return decoder.decode(buffer).toString();
            } catch (CharacterCodingException e) {
                throw new HugeException("Failed to decode truncated bytes of " +
                                        "gremlin first line", e);
            }
        }

        @Override
        public void checkCreate(boolean isBatch) {
            E.checkArgumentNotNull(this.gremlin,
                                   "The gremlin parameter can't be null");
            E.checkArgumentNotNull(this.language,
                                   "The language parameter can't be null");
            E.checkArgument(this.aliases == null || this.aliases.isEmpty(),
                            "There is no need to pass gremlin aliases");
        }

        public String toJson() {
            Map<String, Object> map = new HashMap<>();
            map.put("gremlin", this.gremlin);
            map.put("bindings", this.bindings);
            map.put("language", this.language);
            map.put("aliases", this.aliases);
            return JsonUtil.toJson(map);
        }

        public static GremlinRequest fromJson(String json) {
            @SuppressWarnings("unchecked")
            Map<String, Object> map = JsonUtil.fromJson(json, Map.class);
            String gremlin = (String) map.get("gremlin");
            @SuppressWarnings("unchecked")
            Map<String, Object> bindings = (Map<String, Object>)
                                           map.get("bindings");
            String language = (String) map.get("language");
            @SuppressWarnings("unchecked")
            Map<String, String> aliases = (Map<String, String>)
                                          map.get("aliases");

            GremlinRequest request = new GremlinRequest();
            request.gremlin(gremlin);
            request.bindings(bindings);
            request.language(language);
            request.aliases(aliases);
            return request;
        }
    }
}
