/*
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

package org.apache.hugegraph.api.filter;

import java.io.IOException;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.net.URI;
import java.net.URISyntaxException;

import jakarta.ws.rs.NameBinding;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.core.Response;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.utils.URIBuilder;
import org.apache.hugegraph.election.GlobalMasterInfo;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

public class RedirectFilter implements ContainerRequestFilter {

    private static final Logger LOG = Log.logger(RedirectFilter.class);

    @Override
    public void filter(ContainerRequestContext requestContext) throws IOException {
        GlobalMasterInfo instance = GlobalMasterInfo.instance();
        if (!instance.isFeatureSupport()) {
            return;
        }

        String url = "";
        synchronized (instance) {
            if (instance.isMaster() || StringUtils.isEmpty(instance.url())) {
                return;
            }
            url = instance.url();
        }

        URI redirectUri = null;
        try {
            URIBuilder redirectURIBuilder = new URIBuilder(requestContext.getUriInfo().getAbsolutePath());
            String[] host = url.split(":");
            redirectURIBuilder.setHost(host[0]);
            if (host.length == 2 && StringUtils.isNotEmpty(host[1].trim())) {
                redirectURIBuilder.setPort(Integer.parseInt(host[1].trim()));
            }

            redirectUri = redirectURIBuilder.build();
        } catch (URISyntaxException e) {
            LOG.error("Redirect request exception occurred", e);
            return;
        }
        requestContext.abortWith(Response.temporaryRedirect(redirectUri).build());
    }

    @NameBinding
    @Retention(RetentionPolicy.RUNTIME)
    public @interface RedirectMasterRole {
    }
}
