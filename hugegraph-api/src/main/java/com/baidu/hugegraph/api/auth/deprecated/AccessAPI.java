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

package com.baidu.hugegraph.api.auth.deprecated;

import com.baidu.hugegraph.api.API;
import com.baidu.hugegraph.api.filter.StatusFilter.Status;
import com.baidu.hugegraph.auth.AuthManager;
import com.baidu.hugegraph.auth.HugeAccess;
import com.baidu.hugegraph.auth.HugePermission;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.core.GraphManager;
import com.baidu.hugegraph.define.Checkable;
import com.baidu.hugegraph.exception.NotFoundException;
import com.baidu.hugegraph.server.RestServer;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;
import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
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
import java.util.List;

@Path("graphspaces/{graphspace}/graphs/auth/accesses")
@Singleton
public class AccessAPI extends API {

    private static final Logger LOG = Log.logger(RestServer.class);

    @POST
    @Timed
    @Status(Status.CREATED)
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String create(@Context GraphManager manager,
                         @PathParam("graphspace") String graphSpace,
                         JsonAccess jsonAccess) {
        LOG.debug("Graph space [{}] create access: {}",
                  graphSpace, jsonAccess);
        checkCreatingBody(jsonAccess);

        HugeAccess access = jsonAccess.build(graphSpace);
        AuthManager authManager = manager.authManager();
        access.id(authManager.createAccess(graphSpace, access, true));
        return manager.serializer().writeAuthElement(access);
    }

    @PUT
    @Timed
    @Path("{id}")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String update(@Context GraphManager manager,
                         @PathParam("graphspace") String graphSpace,
                         @PathParam("id") String id,
                         JsonAccess jsonAccess) {
        LOG.debug("Graph space [{}] update access: {}",
                  graphSpace, jsonAccess);
        checkUpdatingBody(jsonAccess);

        HugeAccess access;
        AuthManager authManager = manager.authManager();
        try {
            access = authManager.getAccess(graphSpace,
                                           UserAPI.parseId(id),
                                           true);
        } catch (NotFoundException e) {
            throw new IllegalArgumentException("Invalid access id: " + id);
        }
        access = jsonAccess.build(access);
        authManager.updateAccess(graphSpace, access, true);
        return manager.serializer().writeAuthElement(access);
    }

    @GET
    @Timed
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String list(@Context GraphManager manager,
                       @PathParam("graphspace") String graphSpace,
                       @QueryParam("group") String group,
                       @QueryParam("target") String target,
                       @QueryParam("limit") @DefaultValue("100") long limit) {
        LOG.debug("Graph space [{}] list belongs by group {} or target {}",
                  graphSpace, group, target);
        E.checkArgument(group == null || target == null,
                        "Can't pass both group and target at the same time");

        List<HugeAccess> belongs;
        AuthManager authManager = manager.authManager();
        if (group != null) {
            Id id = UserAPI.parseId(group);
            belongs = authManager.listAccessByGroup(graphSpace, id,
                                                    limit, true);
        } else if (target != null) {
            Id id = UserAPI.parseId(target);
            belongs = authManager.listAccessByTarget(graphSpace, id,
                                                     limit, true);
        } else {
            belongs = authManager.listAllAccess(graphSpace, limit, true);
        }
        return manager.serializer().writeAuthElements("accesses", belongs);
    }

    @GET
    @Timed
    @Path("{id}")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String get(@Context GraphManager manager,
                      @PathParam("graphspace") String graphSpace,
                      @PathParam("id") String id) {
        LOG.debug("Graph space [{}] get access: {}", graphSpace, id);

        AuthManager authManager = manager.authManager();
        HugeAccess access = authManager.getAccess(graphSpace,
                                                  UserAPI.parseId(id),
                                                  true);
        return manager.serializer().writeAuthElement(access);
    }

    @DELETE
    @Timed
    @Path("{id}")
    @Consumes(APPLICATION_JSON)
    public void delete(@Context GraphManager manager,
                       @PathParam("graphspace") String graphSpace,
                       @PathParam("id") String id) {
        LOG.debug("Graph space [{}] delete access: {}", graphSpace, id);

        try {
            AuthManager authManager = manager.authManager();
            authManager.deleteAccess(graphSpace,
                                     UserAPI.parseId(id),
                                     true);
        } catch (NotFoundException e) {
            throw new IllegalArgumentException("Invalid access id: " + id);
        }
    }

    @JsonIgnoreProperties(value = {"id", "access_creator",
                                   "access_create", "access_update"})
    private static class JsonAccess implements Checkable {

        @JsonProperty("group")
        private String group;
        @JsonProperty("target")
        private String target;
        @JsonProperty("access_permission")
        private HugePermission permission;
        @JsonProperty("access_description")
        private String description;

        public HugeAccess build(HugeAccess access) {
            E.checkArgument(this.group == null ||
                            access.source().equals(UserAPI.parseId(this.group)),
                            "The group of access can't be updated");
            E.checkArgument(this.target == null ||
                            access.target().equals(UserAPI.parseId(this.target)),
                            "The target of access can't be updated");
            E.checkArgument(this.permission == null ||
                            access.permission().equals(this.permission),
                            "The permission of access can't be updated");
            if (this.description != null) {
                access.description(this.description);
            }
            return access;
        }

        public HugeAccess build(String graphSpace) {
            HugeAccess access = new HugeAccess(graphSpace,
                                               UserAPI.parseId(this.group),
                                               UserAPI.parseId(this.target));
            access.permission(this.permission);
            access.description(this.description);
            return access;
        }

        @Override
        public void checkCreate(boolean isBatch) {
            E.checkArgumentNotNull(this.group,
                                   "The group of access can't be null");
            E.checkArgumentNotNull(this.target,
                                   "The target of access can't be null");
            E.checkArgumentNotNull(this.permission,
                                   "The permission of access can't be null");
        }

        @Override
        public void checkUpdate() {
        }
    }
}
