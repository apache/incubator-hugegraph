/*
 *
 *  * Copyright 2017 HugeGraph Authors
 *  *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements. See the NOTICE file distributed with this
 *  * work for additional information regarding copyright ownership. The ASF
 *  * licenses this file to You under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance with the
 * License.
 *  * You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  * License for the specific language governing permissions and limitations
 *  * under the License.
 *
 */

package com.baidu.hugegraph.api;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Singleton;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

import org.apache.commons.lang3.StringUtils;
import org.apache.tinkerpop.shaded.jackson.annotation.JsonProperty;
import org.glassfish.jersey.server.model.Parameter.Source;
import org.glassfish.jersey.server.model.Resource;
import org.glassfish.jersey.server.model.ResourceMethod;

import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.JsonUtil;
import com.codahale.metrics.annotation.Timed;

@Path("/")
@Singleton
public class APIList {

    @GET
    @Timed
    @Produces(MediaType.APPLICATION_JSON)
    public String showAll(@Context Application application) {
        Map<String, Map<String, List<APIProfile>>> apiProfiles = new HashMap<>();

        for (Class<?> aClass : application.getClasses()) {
            if (!isAnnotatedResourceClass(aClass)) {
                continue;
            }

            Resource resource = Resource.from(aClass);
            String fullName = resource.getName();
            APICategory apiCategory = getCategory(fullName);

            Map<String, List<APIProfile>> subApiProfiles;
            subApiProfiles = apiProfiles.computeIfAbsent(apiCategory.category,
                                                         k -> new HashMap<>());
            List<APIProfile> profiles = new ArrayList<>();
            subApiProfiles.put(apiCategory.subCategory, profiles);

            String url = resource.getPath();
            // List all methods of this resource
            for (ResourceMethod rm : resource.getResourceMethods()) {
                APIProfile profile = APIProfile.parse(url, rm);
                profiles.add(profile);
            }
            // List all methods of this resource's child resources
            for (Resource childResource : resource.getChildResources()) {
                String childUrl = url + "/" + childResource.getPath();
                for (ResourceMethod rm : childResource.getResourceMethods()) {
                    APIProfile profile = APIProfile.parse(childUrl, rm);
                    profiles.add(profile);
                }
            }
        }
        return JsonUtil.toJson(apiProfiles);
    }

    private static boolean isAnnotatedResourceClass(Class rc) {
        if (rc.isAnnotationPresent(Path.class)) {
            return true;
        }
        for (Class i : rc.getInterfaces()) {
            if (i.isAnnotationPresent(Path.class)) {
                return true;
            }
        }
        return false;
    }

    private static APICategory getCategory(String fullName) {
        String[] parts = StringUtils.split(fullName, ".");
        E.checkState(parts.length >= 2, "Invalid api name");
        String category = parts[parts.length - 2];
        String subCategory = parts[parts.length - 1];
        return new APICategory(category, subCategory);
    }

    private static class APIProfile {

        @JsonProperty("url")
        private final String url;
        @JsonProperty("method")
        private final String method;
        @JsonProperty("parameters")
        private final List<Parameter> parameters;

        public APIProfile(String url, String method,
                          List<Parameter> parameters) {
            this.url = url;
            this.method = method;
            this.parameters = parameters;
        }

        public static APIProfile parse(String url, ResourceMethod rm) {
            String method = rm.getHttpMethod();
            List<Parameter> apiParameters = new ArrayList<>();
            for (org.glassfish.jersey.server.model.Parameter parameter :
                 rm.getInvocable().getParameters()) {
                String sourceName = parameter.getSourceName();
                if (sourceName == null) {
                    continue;
                }
                if (parameter.getSource() == Source.PATH) {
                    continue;
                }
                String typeName = parameter.getType().getTypeName();
                Parameter apiParameter = new Parameter(sourceName, typeName);
                apiParameters.add(apiParameter);
            }
            return new APIProfile(url, method, apiParameters);
        }

        private static class Parameter {

            @JsonProperty("name")
            private String name;
            @JsonProperty("type")
            private String type;

            public Parameter(String name, String type) {
                this.name = name;
                this.type = type;
            }
        }
    }

    private static class APICategory {

        private String category;
        private String subCategory;

        public APICategory(String category, String subCategory) {
            this.category = category;
            this.subCategory = subCategory;
        }
    }
}
