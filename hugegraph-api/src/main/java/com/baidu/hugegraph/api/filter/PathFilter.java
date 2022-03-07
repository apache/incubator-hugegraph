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

package com.baidu.hugegraph.api.filter;

import static com.baidu.hugegraph.space.GraphSpace.DEFAULT_GRAPH_SPACE_SERVICE_NAME;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Set;

import javax.inject.Singleton;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.PreMatching;
import javax.ws.rs.core.PathSegment;
import javax.ws.rs.core.UriInfo;
import javax.ws.rs.ext.Provider;

import org.slf4j.Logger;

import com.baidu.hugegraph.server.RestServer;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;
import com.google.common.collect.ImmutableSet;

@Provider
@Singleton
@PreMatching
public class PathFilter implements ContainerRequestFilter {

    private static final Logger LOG = Log.logger(RestServer.class);

    private static final String GRAPH_SPACE = "graphspaces";
    private static final String DELIMETER = "/";
    private static final Set<String> WHITE_API_LIST = ImmutableSet.of(
            "",
            "apis",
            "metrics",
            "versions",
            "gremlin",
            "graphs/auth",
            "graphs/auth/users",
            "auth/users",
            "auth/managers",
            "auth",
            "hstore",
            "pd"

    );

    @Override
    public void filter(ContainerRequestContext context)
                throws IOException {
        List<PathSegment> segments = context.getUriInfo().getPathSegments();
        E.checkArgument(segments.size() > 0, "Invalid request uri '%s'",
                        context.getUriInfo().getPath());
        String rootPath = segments.get(0).getPath();

        if (isWhiteAPI(rootPath)) {
            return;
        }

        if (GRAPH_SPACE.equals(rootPath)) {
            return;
        }

        UriInfo uriInfo = context.getUriInfo();
        String path = uriInfo.getBaseUri().getPath() +
                String.join(DELIMETER, GRAPH_SPACE, DEFAULT_GRAPH_SPACE_SERVICE_NAME);
        for (PathSegment segment : segments) {
            path = String.join(DELIMETER, path, segment.getPath());
        }
        LOG.debug("Redirect request uri from {} to {}",
                  uriInfo.getRequestUri().getPath(), path);
        URI requestUri = uriInfo.getRequestUriBuilder().uri(path).build();
        context.setRequestUri(uriInfo.getBaseUri(), requestUri);
    }

    public static boolean isWhiteAPI(String rootPath) {

        return WHITE_API_LIST.contains(rootPath);
    }
}
