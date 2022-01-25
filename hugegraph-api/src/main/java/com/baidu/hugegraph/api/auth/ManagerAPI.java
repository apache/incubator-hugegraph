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

import javax.annotation.security.RolesAllowed;
import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;

import com.baidu.hugegraph.api.filter.StatusFilter;
import com.baidu.hugegraph.auth.AuthManager;
import com.baidu.hugegraph.auth.HugePermission;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;

import com.baidu.hugegraph.api.API;
import com.baidu.hugegraph.core.GraphManager;
import com.baidu.hugegraph.define.Checkable;
import com.baidu.hugegraph.server.RestServer;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;
import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.annotation.JsonProperty;

@Path("auth/managers")
@Singleton
public class ManagerAPI extends API {

    private static final Logger LOG = Log.logger(RestServer.class);

    @POST
    @Timed
    @StatusFilter.Status(StatusFilter.Status.CREATED)
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"admin"})
    public String createManager(@Context GraphManager manager,
                                     JsonManager jsonManager) {
        LOG.debug("Create manager: {}", jsonManager);

        String user = jsonManager.user;
        HugePermission type = jsonManager.type;
        String graphSpace = jsonManager.graphSpace;
        AuthManager authManager = manager.authManager();
        E.checkArgument(type == HugePermission.SPACE ||
                        type == HugePermission.ADMIN,
                        "The type could be 'SPACE' or 'ADMIN'");
        E.checkArgument(authManager.findUser(user, false) != null,
                        "The user is not exist");

        if (type == HugePermission.SPACE) {
            E.checkArgument(manager.graphSpace(graphSpace) != null,
                            "The graph space is not exist");

            authManager.createSpaceManager(graphSpace, user);
        } else {
            authManager.createAdminManager(user);
        }

        return manager.serializer()
                      .writeMap(ImmutableMap.of("user", user, "type", type,
                                                "graphspace", graphSpace));
    }

    @DELETE
    @Timed
    @Consumes(APPLICATION_JSON)
    @RolesAllowed({"admin"})
    public void delete(@Context GraphManager manager,
                       @QueryParam("user") String user,
                       @QueryParam("type") HugePermission type,
                       @QueryParam("graphspace") String graphSpace) {
        LOG.debug("Delete graph manager: {} {} {}", user, type, graphSpace);

        AuthManager authManager = manager.authManager();
        E.checkArgument(type == HugePermission.SPACE ||
                        type == HugePermission.ADMIN,
                        "The type could be 'SPACE' or 'ADMIN'");
        E.checkArgument(authManager.findUser(user, false) != null,
                        "The user is not exist");

        if (type == HugePermission.SPACE) {
            E.checkArgument(manager.graphSpace(graphSpace) != null,
                            "The graph space is not exist");

            authManager.deleteSpaceManager(graphSpace, user);
        } else {
            authManager.deleteAdminManager(user);
        }
    }

    private static class JsonManager implements Checkable {

        @JsonProperty("user")
        private String user;
        @JsonProperty("type")
        private HugePermission type;
        @JsonProperty("graphspace")
        private String graphSpace = "";

        @Override
        public void checkCreate(boolean isBatch) {}

        @Override
        public void checkUpdate() {}
    }
}
