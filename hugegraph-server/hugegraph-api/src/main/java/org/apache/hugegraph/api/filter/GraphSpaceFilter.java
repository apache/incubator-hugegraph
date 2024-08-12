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

package org.apache.hugegraph.api.filter;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.stream.Collectors;

import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.config.ServerOptions;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

import jakarta.inject.Singleton;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.container.PreMatching;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.UriBuilder;
import jakarta.ws.rs.ext.Provider;

/**
 * TODO: Change the adaptor logic to keep compatibility with the non-"GraphSpace" version after we
 * support "GraphSpace"
 */
@Provider
@Singleton
@PreMatching
public class GraphSpaceFilter implements ContainerRequestFilter {

    private static final Logger LOG = Log.logger(GraphSpaceFilter.class);

    private static final String GRAPHSPACES_PATH = "graphspaces/";

    @Context
    private jakarta.inject.Provider<HugeConfig> configProvider;

    /**
     * Filters incoming HTTP requests to modify the request URI if it matches certain criteria.
     * <p>
     * This filter checks if the request URI starts with the {@link #GRAPHSPACES_PATH} path
     * segment. If it does,
     * the filter removes the {@link #GRAPHSPACES_PATH} segment along with the following segment
     * and then reconstructs
     * the remaining URI. The modified URI is set back into the request context. This is useful for
     * supporting legacy paths or adapting to new API structures.
     * </p>
     *
     * <p><b>Example:</b></p>
     * <pre>
     * URI baseUri = URI.create("http://localhost:8080/");
     * URI requestUri = URI.create("http://localhost:8080/graphspaces/DEFAULT/graphs");
     *
     * // Before filter:
     * context.getUriInfo().getRequestUri();  // returns http://localhost:8080/graphspaces/DEFAULT/graphs
     *
     * // After filter:
     * context.getUriInfo().getRequestUri();  // returns http://localhost:8080/graphs
     * </pre>
     *
     * @param context The {@link ContainerRequestContext} which provides access to the request
     *                details.
     * @throws IOException If an input or output exception occurs.
     */
    @Override
    public void filter(ContainerRequestContext context) throws IOException {
        HugeConfig config = configProvider.get();
        if (!config.get(ServerOptions.REST_SERVER_ENABLE_GRAPHSPACES_FILTER)) {
            return;
        }

        // Step 1: Get relativePath
        URI baseUri = context.getUriInfo().getBaseUri();
        URI requestUri = context.getUriInfo().getRequestUri();
        URI relativePath = baseUri.relativize(requestUri);

        String relativePathStr = relativePath.getPath();
        // TODO: remember remove the logic after we support "GraphSpace"
        if (!relativePathStr.startsWith(GRAPHSPACES_PATH)) {
            return;
        }

        // Step 2: Extract the next substring after {@link #GRAPHSPACES_PATH}
        String[] parts = relativePathStr.split("/");
        if (parts.length <= 1) {
            return;
        }

        String ignoredPart = Arrays.stream(parts)
                                   .limit(2) // Ignore the first two segments
                                   .collect(Collectors.joining("/"));

        // Reconstruct the remaining path
        String newPath = Arrays.stream(parts)
                               .skip(2) // Skip the first two segments
                               .collect(Collectors.joining("/"));

        // Step 3: Modify RequestUri and log the ignored part
        URI newUri = UriBuilder.fromUri(baseUri)
                               .path(newPath)
                               .replaceQuery(requestUri.getRawQuery())
                               .build();
        context.setRequestUri(newUri);

        // Log the ignored part
        if (LOG.isDebugEnabled()) {
            LOG.debug("Ignored graphspaces segment: {}", ignoredPart);
        }
    }
}
