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
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.utils.URIBuilder;
import org.apache.hugegraph.core.GraphManager;
import org.apache.hugegraph.masterelection.GlobalMasterInfo;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Log;
import org.glassfish.hk2.api.IterableProvider;
import org.glassfish.hk2.api.ServiceHandle;
import org.glassfish.jersey.message.internal.HeaderUtils;
import org.slf4j.Logger;

import jakarta.ws.rs.NameBinding;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.ClientBuilder;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.client.Invocation;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MultivaluedMap;
import jakarta.ws.rs.core.Response;

public class RedirectFilter implements ContainerRequestFilter {

    private static final Logger LOG = Log.logger(RedirectFilter.class);

    private static final String X_HG_REDIRECT = "x-hg-redirect";

    private static volatile Client client = null;

    @Context
    private IterableProvider<GraphManager> managerProvider;

    private static final Set<String> MUST_BE_NULL = new HashSet<>();

    static {
        MUST_BE_NULL.add("DELETE");
        MUST_BE_NULL.add("GET");
        MUST_BE_NULL.add("HEAD");
        MUST_BE_NULL.add("TRACE");
    }

    @Override
    public void filter(ContainerRequestContext context) throws IOException {
        ServiceHandle<GraphManager> handle = this.managerProvider.getHandle();
        E.checkState(handle != null, "Context GraphManager is absent");
        GraphManager manager = handle.getService();
        E.checkState(manager != null, "Context GraphManager is absent");

        String redirectTag = context.getHeaderString(X_HG_REDIRECT);
        if (StringUtils.isNotEmpty(redirectTag)) {
            return;
        }

        GlobalMasterInfo globalNodeInfo = manager.globalNodeRoleInfo();
        if (globalNodeInfo == null || !globalNodeInfo.supportElection()) {
            return;
        }
        GlobalMasterInfo.NodeInfo masterInfo = globalNodeInfo.masterInfo();
        if (masterInfo == null || masterInfo.isMaster() ||
            StringUtils.isEmpty(masterInfo.nodeUrl())) {
            return;
        }
        String url = masterInfo.nodeUrl();

        URI redirectUri;
        try {
            URIBuilder redirectURIBuilder = new URIBuilder(context.getUriInfo().getRequestUri());
            URI masterURI = URI.create(url);
            redirectURIBuilder.setHost(masterURI.getHost());
            redirectURIBuilder.setPort(masterURI.getPort());
            redirectURIBuilder.setScheme(masterURI.getScheme());

            redirectUri = redirectURIBuilder.build();
        } catch (URISyntaxException e) {
            LOG.error("Redirect request exception occurred", e);
            return;
        }
        this.initClientIfNeeded();
        Response response = this.forwardRequest(context, redirectUri);
        context.abortWith(response);
    }

    private Response forwardRequest(ContainerRequestContext requestContext, URI redirectUri) {
        MultivaluedMap<String, String> headers = requestContext.getHeaders();
        MultivaluedMap<String, Object> newHeaders = HeaderUtils.createOutbound();
        if (headers != null) {
            for (Map.Entry<String, List<String>> entry : headers.entrySet()) {
                for (String value : entry.getValue()) {
                    newHeaders.add(entry.getKey(), value);
                }
            }
        }
        newHeaders.add(X_HG_REDIRECT, new Date().getTime());
        Invocation.Builder builder = client.target(redirectUri)
                                           .request()
                                           .headers(newHeaders);
        Response response;
        if (MUST_BE_NULL.contains(requestContext.getMethod())) {
            response = builder.method(requestContext.getMethod());
        } else {
            response = builder.method(requestContext.getMethod(),
                                      Entity.json(requestContext.getEntityStream()));
        }
        return response;
    }

    private void initClientIfNeeded() {
        if (client != null) {
            return;
        }

        synchronized (RedirectFilter.class) {
            if (client != null) {
                return;
            }
            client = ClientBuilder.newClient();
        }
    }

    @NameBinding
    @Retention(RetentionPolicy.RUNTIME)
    public @interface RedirectMasterRole {
    }
}
