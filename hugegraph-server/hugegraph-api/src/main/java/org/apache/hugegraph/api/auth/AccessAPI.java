/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hugegraph.api.auth;

import java.util.List;

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.api.API;
import org.apache.hugegraph.api.filter.StatusFilter.Status;
import org.apache.hugegraph.auth.HugeAccess;
import org.apache.hugegraph.auth.HugePermission;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.core.GraphManager;
import org.apache.hugegraph.define.Checkable;
import org.apache.hugegraph.exception.NotFoundException;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.inject.Singleton;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Context;

@Path("graphspaces/{graphspace}/graphs/{graph}/auth/accesses")
@Singleton
@Tag(name = "AccessAPI")
public class AccessAPI extends API {

    private static final Logger LOG = Log.logger(AccessAPI.class);

    @POST
    @Timed
    @Status(Status.CREATED)
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String create(@Context GraphManager manager,
                         @PathParam("graphspace") String graphSpace,
                         @PathParam("graph") String graph,
                         JsonAccess jsonAccess) {
        LOG.debug("Graph [{}] create access: {}", graph, jsonAccess);
        checkCreatingBody(jsonAccess);

        HugeGraph g = graph(manager, graphSpace, graph);
        HugeAccess access = jsonAccess.build();
        access.id(manager.authManager().createAccess(access));
        return manager.serializer(g).writeAuthElement(access);
    }

    @PUT
    @Timed
    @Path("{id}")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String update(@Context GraphManager manager,
                         @PathParam("graphspace") String graphSpace,
                         @PathParam("graph") String graph,
                         @PathParam("id") String id,
                         JsonAccess jsonAccess) {
        LOG.debug("Graph [{}] update access: {}", graph, jsonAccess);
        checkUpdatingBody(jsonAccess);

        HugeGraph g = graph(manager, graphSpace, graph);
        HugeAccess access;
        try {
            access = manager.authManager().getAccess(UserAPI.parseId(id));
        } catch (NotFoundException e) {
            throw new IllegalArgumentException("Invalid access id: " + id);
        }
        access = jsonAccess.build(access);
        manager.authManager().updateAccess(access);
        return manager.serializer(g).writeAuthElement(access);
    }

    @GET
    @Timed
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String list(@Context GraphManager manager,
                       @PathParam("graphspace") String graphSpace,
                       @PathParam("graph") String graph,
                       @QueryParam("group") String group,
                       @QueryParam("target") String target,
                       @QueryParam("limit") @DefaultValue("100") long limit) {
        LOG.debug("Graph [{}] list belongs by group {} or target {}",
                  graph, group, target);
        E.checkArgument(group == null || target == null,
                        "Can't pass both group and target at the same time");

        HugeGraph g = graph(manager, graphSpace, graph);
        List<HugeAccess> belongs;
        if (group != null) {
            Id id = UserAPI.parseId(group);
            belongs = manager.authManager().listAccessByGroup(id, limit);
        } else if (target != null) {
            Id id = UserAPI.parseId(target);
            belongs = manager.authManager().listAccessByTarget(id, limit);
        } else {
            belongs = manager.authManager().listAllAccess(limit);
        }
        return manager.serializer(g).writeAuthElements("accesses", belongs);
    }

    @GET
    @Timed
    @Path("{id}")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String get(@Context GraphManager manager,
                      @PathParam("graphspace") String graphSpace,
                      @PathParam("graph") String graph,
                      @PathParam("id") String id) {
        LOG.debug("Graph [{}] get access: {}", graph, id);

        HugeGraph g = graph(manager, graphSpace, graph);
        HugeAccess access = manager.authManager().getAccess(UserAPI.parseId(id));
        return manager.serializer(g).writeAuthElement(access);
    }

    @DELETE
    @Timed
    @Path("{id}")
    @Consumes(APPLICATION_JSON)
    public void delete(@Context GraphManager manager,
                       @PathParam("graphspace") String graphSpace,
                       @PathParam("graph") String graph,
                       @PathParam("id") String id) {
        LOG.debug("Graph [{}] delete access: {}", graph, id);

        @SuppressWarnings("unused") // just check if the graph exists
        HugeGraph g = graph(manager, graphSpace, graph);
        try {
            manager.authManager().deleteAccess(UserAPI.parseId(id));
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

        public HugeAccess build() {
            HugeAccess access = new HugeAccess(UserAPI.parseId(this.group),
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
            E.checkArgumentNotNull(this.description,
                                   "The description of access can't be null");
        }
    }
}
