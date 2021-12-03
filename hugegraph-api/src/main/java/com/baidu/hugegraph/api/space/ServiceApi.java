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

import java.util.Set;

import javax.annotation.security.RolesAllowed;
import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.SecurityContext;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import com.baidu.hugegraph.api.API;
import com.baidu.hugegraph.api.filter.StatusFilter.Status;
import com.baidu.hugegraph.core.GraphManager;
import com.baidu.hugegraph.define.Checkable;
import com.baidu.hugegraph.server.RestServer;
import com.baidu.hugegraph.space.Service;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;
import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

@Path("graphspaces/{graphspace}/services")
@Singleton
public class ServiceApi extends API {

    private static final Logger LOG = Log.logger(RestServer.class);

    private static final String CONFIRM_DROP = "I'm sure to delete the service";

    private static final String CLUSTER_IP = "ClusterIP";
    private static final String LOAD_BALANCER = "LoadBalancer";
    private static final String NODE_PORT = "NodePort";

    @GET
    @Timed
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"admin", "$dynamic"})
    public Object list(@Context GraphManager manager,
                       @PathParam("graphspace") String graphSpace,
                       @Context SecurityContext sc) {
        LOG.debug("List all services for graph space {}", graphSpace);

        Set<String> services = manager.services(graphSpace);
        return ImmutableMap.of("services", services);
    }

    @GET
    @Timed
    @Path("{name}")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public Object get(@Context GraphManager manager,
                      @PathParam("graphspace") String graphSpace,
                      @PathParam("name") String name) {
        LOG.debug("Get service by name '{}' for graph space {}",
                  name, graphSpace);

        return manager.serializer().writeService(
                                    service(manager, graphSpace, name));
    }

    @POST
    @Timed
    @Status(Status.CREATED)
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String create(@Context GraphManager manager,
                         @PathParam("graphspace") String graphSpace,
                         JsonService jsonService) {
        LOG.debug("Create service {} for graph space: '{}'",
                  jsonService, graphSpace);

        jsonService.checkCreate(false);

        Service service = manager.createService(graphSpace,
                                                jsonService.toService());
        return manager.serializer().writeService(service);
    }

    @DELETE
    @Timed
    @Path("{name}")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"admin"})
    public void delete(@Context GraphManager manager,
                       @PathParam("graphspace") String graphSpace,
                       @PathParam("name") String name,
                       @QueryParam("confirm_message") String message) {
        LOG.debug("Remove service by name '{}' for graph space",
                  name, graphSpace);

        E.checkArgument(CONFIRM_DROP.equals(message),
                        "Please take the message: %s", CONFIRM_DROP);
        manager.dropService(graphSpace, name);
    }

    private static class JsonService implements Checkable {

        @JsonProperty("name")
        public String name;
        @JsonProperty("type")
        public Service.ServiceType serviceType;
        @JsonProperty("description")
        public String description;
        @JsonProperty("count")
        public int count;

        @JsonProperty("cpu_limit")
        public int cpuLimit;
        @JsonProperty("memory_limit")
        public int memoryLimit;
        @JsonProperty("storage_limit")
        public int storageLimit;

        @JsonProperty("route_type")
        public String routeType;
        @JsonProperty("port")
        public int port;

        @Override
        public void checkCreate(boolean isBatch) {
            E.checkArgument(this.name != null &&
                            !StringUtils.isEmpty(this.name),
                            "The name of service can't be null or empty");

            E.checkArgument(this.serviceType != null,
                            "The type of service can't be null");

            E.checkArgument(this.count > 0,
                            "The service count must be > 0, but got: %s",
                            this.count);

            E.checkArgument(this.cpuLimit > 0,
                            "The cpu limit must be > 0, but got: %s",
                            this.cpuLimit);
            E.checkArgument(this.memoryLimit > 0,
                            "The memory limit must be > 0, but got: %s",
                            this.memoryLimit);
            E.checkArgument(this.storageLimit > 0,
                            "The storage limit must be > 0, but got: %s",
                            this.storageLimit);

            E.checkArgument(this.routeType != null &&
                            !StringUtils.isEmpty(this.routeType),
                            "The route type of service can't be null or empty");
            E.checkArgument(NODE_PORT.equals(this.routeType) ||
                            CLUSTER_IP.equals(this.routeType) ||
                            LOAD_BALANCER.equals(this.routeType),
                            "Invalid route type '%s'", this.routeType);
            if (isNodePort(this.routeType)) {
                E.checkArgument(this.port > 0,
                                "The port must be > 0, but got: %s",
                                this.port);
            }
        }

        public Service toService() {
            Service service = new Service(this.name, this.serviceType);
            service.description(this.description);
            service.count(this.count);

            service.cpuLimit(this.cpuLimit);
            service.memoryLimit(this.memoryLimit);
            service.storageLimit(this.storageLimit);

            service.routeType(this.routeType);
            if (isNodePort(this.routeType)) {
                service.port(this.port);
            }

            return service;
        }

        public String toString() {
            return String.format("JsonService{name=%s, type=%s, " +
                                 "description=%s, count=%s, cpuLimit=%s, " +
                                 "memoryLimit=%s, storageLimit=%s, port=%s}",
                                 this.name, this.serviceType, this.description,
                                 this.count, this.cpuLimit, this.memoryLimit,
                                 this.storageLimit, this.port);
        }

        public static boolean isNodePort(String routeType) {
            return NODE_PORT.equals(routeType);
        }
    }
}
