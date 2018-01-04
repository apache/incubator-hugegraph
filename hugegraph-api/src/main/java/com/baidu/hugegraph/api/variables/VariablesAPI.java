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

package com.baidu.hugegraph.api.variables;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;

import org.slf4j.Logger;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.api.API;
import com.baidu.hugegraph.core.GraphManager;
import com.baidu.hugegraph.server.RestServer;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;
import com.google.common.collect.ImmutableMap;

@Path("graphs/{graph}/variables")
@Singleton
public class VariablesAPI extends API {

    private static final Logger LOG = Log.logger(RestServer.class);

    @PUT
    @Path("{key}")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public Map<String, Object> update(@Context GraphManager manager,
                                      @PathParam("graph") String graph,
                                      @PathParam("key") String key,
                                      JsonVariableValue value) {
        E.checkArgument(value != null && value.data != null,
                        "The variable value can't be empty");
        LOG.debug("Graph [{}] set variable for {}: {}", graph, key, value);

        HugeGraph g = graph(manager, graph);
        commit(g, () -> g.variables().set(key, value.data));
        return ImmutableMap.of(key, value);
    }

    @GET
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public Map<String, Object> list(@Context GraphManager manager,
                                    @PathParam("graph") String graph) {
        LOG.debug("Graph [{}] get variables", graph);

        HugeGraph g = graph(manager, graph);
        return g.variables().asMap();
    }

    @GET
    @Path("{key}")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public Map<String, Object> get(@Context GraphManager manager,
                                   @PathParam("graph") String graph,
                                   @PathParam("key") String key) {
        LOG.debug("Graph [{}] get variable by key '{}'", graph, key);

        HugeGraph g = graph(manager, graph);
        Map<String, Object> result = new HashMap<>();
        Optional<?> object = g.variables().get(key);
        if (!object.isPresent()) {
            throw new NotFoundException(String.format(
                      "Variable '%s' does not exist", key));
        }
        result.put(key, object.get());
        return result;
    }

    @DELETE
    @Path("{key}")
    @Consumes(APPLICATION_JSON)
    public void delete(@Context GraphManager manager,
                       @PathParam("graph") String graph,
                       @PathParam("key") String key) {
        LOG.debug("Graph [{}] remove variable by key '{}'", graph, key);

        HugeGraph g = graph(manager, graph);
        commit(g, () -> g.variables().remove(key));
    }

    private static class JsonVariableValue {

        public Object data;

        @Override
        public String toString() {
            return String.format("JsonVariableValue{data=%s}", this.data);
        }
    }
}
