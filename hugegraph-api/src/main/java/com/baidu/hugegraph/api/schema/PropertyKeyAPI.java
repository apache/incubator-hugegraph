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

import java.util.List;

import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;

import org.slf4j.Logger;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.api.API;
import com.baidu.hugegraph.api.filter.StatusFilter.Status;
import com.baidu.hugegraph.core.GraphManager;
import com.baidu.hugegraph.schema.PropertyKey;
import com.baidu.hugegraph.type.define.Cardinality;
import com.baidu.hugegraph.type.define.DataType;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;
import com.fasterxml.jackson.annotation.JsonProperty;

@Path("graphs/{graph}/schema/propertykeys")
@Singleton
public class PropertyKeyAPI extends API {

    private static final Logger LOG = Log.logger(PropertyKeyAPI.class);

    @POST
    @Status(Status.CREATED)
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String create(@Context GraphManager manager,
                         @PathParam("graph") String graph,
                         JsonPropertyKey jsonPropertyKey) {
        E.checkArgumentNotNull(jsonPropertyKey,
                               "The request body can't be empty");

        LOG.debug("Graph [{}] create property key: {}",
                  graph, jsonPropertyKey);

        HugeGraph g = (HugeGraph) graph(manager, graph);

        PropertyKey propertyKey = jsonPropertyKey.convert2PropertyKey();
        propertyKey = g.schema().propertyKey(propertyKey).create();

        return manager.serializer(g).writePropertyKey(propertyKey);
    }

    @GET
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String list(@Context GraphManager manager,
                       @PathParam("graph") String graph) {
        LOG.debug("Graph [{}] get property keys", graph);

        HugeGraph g = (HugeGraph) graph(manager, graph);
        List<PropertyKey> propKeys = g.schema().getPropertyKeys();

        return manager.serializer(g).writePropertyKeys(propKeys);
    }

    @GET
    @Path("{name}")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String get(@Context GraphManager manager,
                      @PathParam("graph") String graph,
                      @PathParam("name") String name) {
        LOG.debug("Graph [{}] get property key by name '{}'", graph, name);

        HugeGraph g = (HugeGraph) graph(manager, graph);
        PropertyKey propertyKey = g.schema().getPropertyKey(name);
        return manager.serializer(g).writePropertyKey(propertyKey);
    }

    @DELETE
    @Path("{name}")
    @Consumes(APPLICATION_JSON)
    public void delete(@Context GraphManager manager,
                       @PathParam("graph") String graph,
                       @PathParam("name") String name) {
        LOG.debug("Graph [{}] remove property key by name '{}'", graph, name);

        HugeGraph g = (HugeGraph) graph(manager, graph);
        // Just check exists
        g.schema().getPropertyKey(name);
        g.schema().propertyKey(name).remove();
    }

    /**
     * JsonPropertyKey is only used to receive create and append requests
     */
    private static class JsonPropertyKey {

        @JsonProperty("name")
        public String name;
        @JsonProperty("cardinality")
        public Cardinality cardinality;
        @JsonProperty("data_type")
        public DataType dataType;
        @JsonProperty("properties")
        public String[] properties;
        @JsonProperty("check_exist")
        public Boolean checkExist;

        private PropertyKey convert2PropertyKey() {
            E.checkArgumentNotNull(this.name,
                                   "The name of property key can't be null");
            E.checkArgument(this.properties == null ||
                            this.properties.length == 0,
                            "Not allowed to pass properties when " +
                            "creating property key since it doesn't " +
                            "support meta properties currently");
            PropertyKey propertyKey = new PropertyKey(this.name);
            // Weather it can be replacee by java 8 function or consumer
            if (this.cardinality != null) {
                propertyKey.cardinality(this.cardinality);
            }
            if (this.dataType != null) {
                propertyKey.dataType(this.dataType);
            }
            if (this.checkExist != null) {
                propertyKey.checkExist(this.checkExist);
            }
            return propertyKey;
        }

        @Override
        public String toString() {
            return String.format("JsonPropertyKey{name=%s, cardinality=%s, " +
                                 "dataType=%s, properties=%s}",
                                 this.name, this.cardinality,
                                 this.dataType, this.properties);
        }
    }
}
