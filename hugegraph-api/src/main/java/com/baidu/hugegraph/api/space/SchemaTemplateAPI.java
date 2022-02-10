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

import java.util.Date;
import java.util.Set;

import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.SecurityContext;

import com.baidu.hugegraph.auth.HugeAuthenticator;
import com.baidu.hugegraph.auth.HugePermission;
import org.slf4j.Logger;

import com.baidu.hugegraph.api.API;
import com.baidu.hugegraph.api.filter.StatusFilter;
import com.baidu.hugegraph.core.GraphManager;
import com.baidu.hugegraph.define.Checkable;
import com.baidu.hugegraph.server.RestServer;
import com.baidu.hugegraph.space.SchemaTemplate;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;
import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

@Path("graphspaces/{graphspace}/schematemplates")
@Singleton
public class SchemaTemplateAPI extends API {

    private static final Logger LOG = Log.logger(RestServer.class);

    @GET
    @Timed
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public Object list(@Context GraphManager manager,
                       @PathParam("graphspace") String graphSpace) {
        LOG.debug("List all schema templates for graph space {}", graphSpace);

        Set<String> templates = manager.schemaTemplates(graphSpace);
        return ImmutableMap.of("schema_templates", templates);
    }

    @GET
    @Timed
    @Path("{name}")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public Object get(@Context GraphManager manager,
                      @PathParam("graphspace") String graphSpace,
                      @PathParam("name") String name) {
        LOG.debug("Get schema template by name '{}' for graph space {}",
                  name, graphSpace);

        return manager.serializer().writeSchemaTemplate(
                       schemaTemplate(manager, graphSpace, name));
    }

    @POST
    @Timed
    @StatusFilter.Status(StatusFilter.Status.CREATED)
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String create(@Context GraphManager manager,
                         @PathParam("graphspace") String graphSpace,
                         JsonSchemaTemplate jsonSchemaTemplate) {
        LOG.debug("Create schema template {} for graph space: '{}'",
                  jsonSchemaTemplate, graphSpace);
        jsonSchemaTemplate.checkCreate(false);

        E.checkArgument(manager.graphSpace(graphSpace) != null,
                        "The graph space '%s' is not exist", graphSpace);

        SchemaTemplate template = jsonSchemaTemplate.toSchemaTemplate();
        template.create(new Date());
        template.creator(manager.authManager().username());
        manager.createSchemaTemplate(graphSpace, template);
        return manager.serializer().writeSchemaTemplate(template);
    }

    @DELETE
    @Timed
    @Path("{name}")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public void delete(@Context GraphManager manager,
                       @PathParam("graphspace") String graphSpace,
                       @PathParam("name") String name,
                       @Context SecurityContext sc) {
        LOG.debug("Remove schema template by name '{}' for graph space",
                  name, graphSpace);
        E.checkArgument(manager.graphSpace(graphSpace) != null,
                        "The graph space '%s' is not exist", graphSpace);

        SchemaTemplate st = schemaTemplate(manager, graphSpace, name);
        E.checkArgument(st != null,
                        "Schema template '%s' does not exist", name);

        String username = manager.authManager().username();
        String role = HugeAuthenticator.RequiredPerm.roleFor(graphSpace, "*",
                                                             HugePermission.SPACE);
        if (st.creator().equals(username) || sc.isUserInRole(role)) {
            manager.dropSchemaTemplate(graphSpace, name);
        }
    }

    private static class JsonSchemaTemplate implements Checkable {

        @JsonProperty("name")
        public String name;
        @JsonProperty("schema")
        public String schema;

        @Override
        public void checkCreate(boolean isBatch) {
            E.checkArgument(this.name != null && !this.name.isEmpty(),
                            "The name of schema template can't be null or " +
                            "empty");

            E.checkArgument(this.schema != null && !this.schema.isEmpty(),
                            "The schema can't be null or empty");
        }

        public SchemaTemplate toSchemaTemplate() {
            return new SchemaTemplate(this.name, this.schema);
        }

        public String toString() {
            return String.format("JsonSchemaTemplate{name=%s, schema=%s}",
                                 this.name, this.schema);
        }
    }
}
