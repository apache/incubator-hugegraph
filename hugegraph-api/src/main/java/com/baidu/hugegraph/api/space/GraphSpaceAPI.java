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

package com.baidu.hugegraph.api.space;

import static com.baidu.hugegraph.space.GraphSpace.DEFAULT_GRAPH_SPACE_NAME;

import java.util.Map;
import java.util.Set;

import javax.annotation.security.RolesAllowed;
import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;

import com.baidu.hugegraph.api.API;
import com.baidu.hugegraph.api.filter.StatusFilter.Status;
import com.baidu.hugegraph.core.GraphManager;
import com.baidu.hugegraph.define.Checkable;
import com.baidu.hugegraph.server.RestServer;
import com.baidu.hugegraph.space.GraphSpace;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;
import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

@Path("graphspaces")
@Singleton
public class GraphSpaceAPI extends API {

    private static final Logger LOG = Log.logger(RestServer.class);

    private static final String GRAPH_SPACE_ACTION = "action";
    private static final String CONFIRM_MESSAGE = "confirm_message";
    private static final String GRAPH_SPACE_ACTION_CLEAR = "clear";

    private static final String CONFIRM_CLEAR = "I'm sure to delete all data";
    private static final String CONFIRM_DROP = "I'm sure to drop the graph space";

    @GET
    @Timed
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public Object list(@Context GraphManager manager) {
        LOG.debug("Get graphSpaces'");
        Set<String> spaces = manager.graphSpaces();
        return ImmutableMap.of("graphSpaces", spaces);
    }

    @GET
    @Timed
    @Path("{name}")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public Object get(@Context GraphManager manager,
                      @PathParam("name") String name) {
        LOG.debug("Get graph space by name '{}'", name);

        return manager.serializer(null).writeGraphSpace(space(manager, name));
    }

    @POST
    @Timed
    @Status(Status.CREATED)
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public Object create(@Context GraphManager manager,
                         JsonGraphSpace graphSpace) {
        LOG.debug("Create space: '{}'", graphSpace);
        E.checkArgument(!DEFAULT_GRAPH_SPACE_NAME.equals(graphSpace),
                        "Can't create namespace with name '%s'",
                        DEFAULT_GRAPH_SPACE_NAME);
        GraphSpace space = manager.createGraphSpace(graphSpace.name,
                                                    graphSpace.maxGraphNumber,
                                                    graphSpace.maxRoleNumber,
                                                    graphSpace.configs);
        return manager.serializer(null).writeGraphSpace(space);
    }

    @PUT
    @Timed
    @Path("{name}")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed("admin")
    public Map<String, String> clear(@Context GraphManager manager,
                                     @PathParam("name") String name,
                                     Map<String, String> actionMap) {
        LOG.debug("Clear graph space by name '{}'", name);
        E.checkArgument(actionMap != null && actionMap.size() == 2 &&
                        actionMap.containsKey(GRAPH_SPACE_ACTION) &&
                        actionMap.containsKey(CONFIRM_MESSAGE),
                        "Please pass '%s' and '%s' for graph space clear",
                        GRAPH_SPACE_ACTION, CONFIRM_MESSAGE);
        String action = actionMap.get(GRAPH_SPACE_ACTION);
        E.checkArgument(GRAPH_SPACE_ACTION_CLEAR.equals(action),
                        "Not support graph space action: '%s'", action);
        String message = actionMap.get(CONFIRM_MESSAGE);
        E.checkArgument(CONFIRM_CLEAR.equals(message),
                        "Please take the message: %s", CONFIRM_CLEAR);
        manager.clearGraphSpace(name);
        return ImmutableMap.of(name, "cleared");
    }

    @DELETE
    @Timed
    @Path("{name}")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"admin"})
    public void delete(@Context GraphManager manager,
                       @PathParam("name") String name,
                       @QueryParam("confirm_message") String message) {
        LOG.debug("Remove graph space by name '{}'", name);
        E.checkArgument(CONFIRM_DROP.equals(message),
                        "Please take the message: %s", CONFIRM_DROP);
        manager.dropGraphSpace(name);
    }

    private static class JsonGraphSpace implements Checkable {

        @JsonProperty("name")
        public String name;
        @JsonProperty("max_graph_number")
        public int maxGraphNumber;
        @JsonProperty("max_role_number")
        public int maxRoleNumber;
        @JsonProperty("configs")
        public Map<String, Object> configs;



        @Override
        public void checkCreate(boolean isBatch) {
            E.checkArgument(this.name != null &&
                            !StringUtils.isEmpty(this.name),
                            "The name of graph space can't be null or empty");
            E.checkArgument(this.maxGraphNumber > 0,
                            "The max graph number must > 0");
            E.checkArgument(this.maxRoleNumber > 0,
                            "The max role number must > 0");
        }

        public String toString() {
            return String.format("JsonGraphSpace{name=%s, maxGraphNumber=%s, " +
                                 "maxRoleNumber=%s, configs=%s}",
                                 this.name, this.maxGraphNumber,
                                 this.maxRoleNumber, this.configs);
        }
    }
}
