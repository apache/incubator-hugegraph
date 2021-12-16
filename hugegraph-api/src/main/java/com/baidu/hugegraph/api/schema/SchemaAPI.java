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
import java.util.Set;

import javax.annotation.security.RolesAllowed;
import javax.inject.Singleton;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;

import org.slf4j.Logger;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.api.API;
import com.baidu.hugegraph.core.GraphManager;
import com.baidu.hugegraph.schema.EdgeLabel;
import com.baidu.hugegraph.schema.IndexLabel;
import com.baidu.hugegraph.schema.PropertyKey;
import com.baidu.hugegraph.schema.SchemaManager;
import com.baidu.hugegraph.schema.VertexLabel;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;
import com.codahale.metrics.annotation.Timed;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

@Path("graphspaces/{graphspace}/graphs/{graph}/schema")
@Singleton
public class SchemaAPI extends API {

    private static final Logger LOG = Log.logger(SchemaAPI.class);

    private static final String JSON = "json";
    private static final String GROOVY = "groovy";

    private static final Set<String> FORMATS = ImmutableSet.of(JSON, GROOVY);

    @GET
    @Timed
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"admin", "$graphspace=$graphspace $owner=$graph " +
                            "$action=schema_read"})
    public String list(@Context GraphManager manager,
                       @PathParam("graphspace") String graphSpace,
                       @PathParam("graph") String graph,
                       @QueryParam("format")
                       @DefaultValue("json") String format) {
        LOG.debug("Graph [{}] list all schema with format '{}'", graph, format);

        E.checkArgument(FORMATS.contains(format),
                        "Invalid format '%s', valid format is '%s' or '%s'",
                        format, JSON, GROOVY);
        HugeGraph g = graph(manager, graphSpace, graph);
        SchemaManager schema = g.schema();

        List<PropertyKey> propertyKeys = schema.getPropertyKeys();
        List<VertexLabel> vertexLabels = schema.getVertexLabels();
        List<EdgeLabel> edgeLabels = schema.getEdgeLabels();
        List<IndexLabel> indexLabels = schema.getIndexLabels();

        if (JSON.equals(format)) {
            Map<String, List<?>> schemaMap = new LinkedHashMap<>(4);
            schemaMap.put("propertykeys", propertyKeys);
            schemaMap.put("vertexlabels", vertexLabels);
            schemaMap.put("edgelabels", edgeLabels);
            schemaMap.put("indexlabels", indexLabels);
            return manager.serializer().writeMap(schemaMap);
        } else {
            StringBuilder builder = new StringBuilder();
            for (PropertyKey propertyKey : propertyKeys) {
                builder.append(propertyKey.convert2Groovy())
                       .append("\n");
            }
            if (!propertyKeys.isEmpty()) {
                builder.append("\n");
            }
            for (VertexLabel vertexLabel : vertexLabels) {
                builder.append(vertexLabel.convert2Groovy())
                       .append("\n");
            }
            if (!vertexLabels.isEmpty()) {
                builder.append("\n");
            }
            for (EdgeLabel edgeLabel : edgeLabels) {
                builder.append(edgeLabel.convert2Groovy())
                       .append("\n");
            }
            if (!edgeLabels.isEmpty()) {
                builder.append("\n");
            }
            for (IndexLabel indexLabel : indexLabels) {
                builder.append(indexLabel.convert2Groovy())
                       .append("\n");
            }
            return manager.serializer().writeMap(
                           ImmutableMap.of("schema", builder.toString()));
        }
    }
}
