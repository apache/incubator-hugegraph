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

import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

import jakarta.inject.Singleton;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.container.PreMatching;
import jakarta.ws.rs.core.UriBuilder;
import jakarta.ws.rs.ext.Provider;

/** TODO: Change the adaptor logic to keep compatibility with the non-gs version after we support gs function ↓ */
@Provider
@Singleton
@PreMatching
public class GraphSpaceFilter implements ContainerRequestFilter {

    private static final Logger LOG = Log.logger(GraphSpaceFilter.class);

    @Override
    public void filter(ContainerRequestContext context) throws IOException {
        // Step 1: Get relativePath
        URI baseUri = context.getUriInfo().getBaseUri();
        URI requestUri = context.getUriInfo().getRequestUri();
        URI relativePath = baseUri.relativize(requestUri);

        String relativePathStr = relativePath.getPath();
        // TODO: remember change the logic after we support "GraphSpace"
        if (relativePathStr.startsWith("graphspaces/")) {
            // Step 2: Extract the next substring after "graphspaces/"
            String[] parts = relativePathStr.split("/");
            if (parts.length > 2) {
                String ignoredPart = parts[1];
                String nextPathPart = parts[2];

                // Step 3: Modify RequestUri and log the ignored part
                URI newUri = UriBuilder.fromUri(baseUri)
                                       .path(nextPathPart)
                                       .build();

                context.setRequestUri(newUri);

                // Log the ignored part
                LOG.info("Ignored graphspaces segment: {}", ignoredPart);
            }
        }
    }
}
