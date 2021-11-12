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

package com.baidu.hugegraph.api.schema;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.security.RolesAllowed;
import javax.inject.Singleton;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;

import org.slf4j.Logger;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.api.API;
import com.baidu.hugegraph.core.GraphManager;
import com.baidu.hugegraph.schema.SchemaManager;
import com.baidu.hugegraph.util.Log;
import com.codahale.metrics.annotation.Timed;

@Path("graphspaces/{graphspace}/graphs/{graph}/schema")
@Singleton
public class SchemaAPI extends API {

    private static final Logger LOG = Log.logger(SchemaAPI.class);

    @GET
    @Timed
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"admin", "$graphspace=$graphspace $owner=$graph " +
                            "$action=schema_read"})
    public String list(@Context GraphManager manager,
                       @PathParam("graphspace") String graphSpace,
                       @PathParam("graph") String graph) {
        LOG.debug("Graph [{}] list all schema", graph);

        HugeGraph g = graph(manager, graphSpace, graph);
        SchemaManager schema = g.schema();

        Map<String, List<?>> schemaMap = new LinkedHashMap<>(4);
        schemaMap.put("propertykeys", schema.getPropertyKeys());
        schemaMap.put("vertexlabels", schema.getVertexLabels());
        schemaMap.put("edgelabels", schema.getEdgeLabels());
        schemaMap.put("indexlabels", schema.getIndexLabels());

        return manager.serializer().writeMap(schemaMap);
    }
}
