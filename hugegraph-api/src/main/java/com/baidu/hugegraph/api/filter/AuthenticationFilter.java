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

import java.io.IOException;
import java.security.Principal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Priority;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.NotAuthorizedException;
import javax.ws.rs.Priorities;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.PreMatching;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.SecurityContext;
import javax.ws.rs.core.UriInfo;
import javax.ws.rs.ext.Provider;
import javax.xml.bind.DatatypeConverter;

import org.apache.commons.lang3.StringUtils;
import org.apache.tinkerpop.gremlin.server.auth.AuthenticationException;
import org.glassfish.grizzly.http.server.Request;
import org.glassfish.grizzly.utils.Charsets;
import org.slf4j.Logger;

import com.baidu.hugegraph.auth.HugeAuthenticator;
import com.baidu.hugegraph.auth.HugeAuthenticator.RequiredPerm;
import com.baidu.hugegraph.auth.HugeAuthenticator.RolePerm;
import com.baidu.hugegraph.auth.HugeAuthenticator.User;
import com.baidu.hugegraph.auth.RolePermission;
import com.baidu.hugegraph.core.GraphManager;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;
import com.google.common.collect.ImmutableList;

@Provider
@PreMatching
@Priority(Priorities.AUTHENTICATION)
public class AuthenticationFilter implements ContainerRequestFilter {

    public static final String BASIC_AUTH_PREFIX = "Basic ";
    public static final String BEARER_TOKEN_PREFIX = "Bearer ";

    private static final Logger LOG = Log.logger(AuthenticationFilter.class);

    private static final List<String> WHITE_API_LIST = ImmutableList.of(
            "auth/login",
            "versions"
    );

    @Context
    private javax.inject.Provider<GraphManager> managerProvider;

    @Context
    private javax.inject.Provider<Request> requestProvider;

    @Override
    public void filter(ContainerRequestContext context) throws IOException {
        if (AuthenticationFilter.isWhiteAPI(context)) {
            return;
        }
        User user = this.authenticate(context);
        Authorizer authorizer = new Authorizer(user, context.getUriInfo());
        context.setSecurityContext(authorizer);
    }

    protected User authenticate(ContainerRequestContext context) {
        GraphManager manager = this.managerProvider.get();
        E.checkState(manager != null, "Context GraphManager is absent");

        if (!manager.requireAuthentication()) {
            // Return anonymous user with admin role if disable authentication
            return User.ANONYMOUS;
        }

        // Get peer info
        Request request = this.requestProvider.get();
        String peer = null;
        String path = null;
        if (request != null) {
            peer = request.getRemoteAddr() + ":" + request.getRemotePort();
            path = request.getRequestURI();
        }

        Map<String, String> credentials = new HashMap<>();
        // Extract authentication credentials
        String auth = context.getHeaderString(HttpHeaders.AUTHORIZATION);
        if (auth == null) {
            throw new NotAuthorizedException(
                      "Authentication credentials are required",
                      "Missing authentication credentials");
        }

        if (auth.startsWith(BASIC_AUTH_PREFIX)) {
            auth = auth.substring(BASIC_AUTH_PREFIX.length());
            auth = new String(DatatypeConverter.parseBase64Binary(auth),
                              Charsets.ASCII_CHARSET);
            String[] values = auth.split(":");
            if (values.length != 2) {
                throw new BadRequestException(
                          "Invalid syntax for username and password");
            }

            final String username = values[0];
            final String password = values[1];

            if (StringUtils.isEmpty(username) ||
                StringUtils.isEmpty(password)) {
                throw new BadRequestException(
                          "Invalid syntax for username and password");
            }

            credentials.put(HugeAuthenticator.KEY_USERNAME, username);
            credentials.put(HugeAuthenticator.KEY_PASSWORD, password);
        } else if (auth.startsWith(BEARER_TOKEN_PREFIX)) {
            String token = auth.substring(BEARER_TOKEN_PREFIX.length());
            credentials.put(HugeAuthenticator.KEY_TOKEN, token);
        } else {
            throw new BadRequestException(
                      "Only HTTP Basic or Bearer authentication is supported");
        }

        credentials.put(HugeAuthenticator.KEY_ADDRESS, peer);
        credentials.put(HugeAuthenticator.KEY_PATH, path);

        // Validate the extracted credentials
        try {
            return manager.authenticate(credentials);
        } catch (AuthenticationException e) {
            throw new NotAuthorizedException("Authentication failed",
                                             e.getMessage());
        }
    }

    public static class Authorizer implements SecurityContext {

        private final UriInfo uri;
        private final User user;
        private final Principal principal;

        public Authorizer(final User user, final UriInfo uri) {
            E.checkNotNull(user, "user");
            E.checkNotNull(uri, "uri");
            this.uri = uri;
            this.user = user;
            this.principal = new UserPrincipal();
        }

        public String username() {
            return this.user.username();
        }

        public RolePermission role() {
            return this.user.role();
        }

        @Override
        public Principal getUserPrincipal() {
            return this.principal;
        }

        @Override
        public boolean isUserInRole(String required) {
            if (required.equals(HugeAuthenticator.KEY_DYNAMIC)) {
                // Let the resource itself determine dynamically
                return true;
            } else {
                return this.matchPermission(required);
            }
        }

        @Override
        public boolean isSecure() {
            return "https".equals(this.uri.getRequestUri().getScheme());
        }

        @Override
        public String getAuthenticationScheme() {
            return SecurityContext.BASIC_AUTH;
        }

        private boolean matchPermission(String required) {
            boolean valid;
            RequiredPerm requiredPerm;

            if (!required.startsWith(HugeAuthenticator.KEY_OWNER)) {
                // Permission format like: "admin"
                requiredPerm = new RequiredPerm();
                requiredPerm.owner(required);
            } else {
                // The required like: $owner=graph1 $action=vertex_write
                requiredPerm = RequiredPerm.fromPermission(required);

                /*
                 * Replace owner value(it may be a variable) if the permission
                 * format like: "$owner=$graph $action=vertex_write"
                 */
                String owner = requiredPerm.owner();
                if (owner.startsWith(HugeAuthenticator.VAR_PREFIX)) {
                    // Replace `$graph` with graph name like "graph1"
                    int prefixLen = HugeAuthenticator.VAR_PREFIX.length();
                    assert owner.length() > prefixLen;
                    owner = owner.substring(prefixLen);
                    owner = this.getPathParameter(owner);
                    requiredPerm.owner(owner);
                }
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("Verify permission {} {} for user '{}' with role {}",
                          requiredPerm.action().string(),
                          requiredPerm.resourceObject(),
                          this.user.username(), this.user.role());
            }

            // verify role permission
            valid = RolePerm.match(this.role(), requiredPerm);

            if (!valid && LOG.isInfoEnabled() &&
                !required.equals(HugeAuthenticator.USER_ADMIN)) {
                LOG.info("User '{}' is denied to {} {}",
                         this.user.username(), requiredPerm.action().string(),
                         requiredPerm.resourceObject());
            }
            return valid;
        }

        private String getPathParameter(String key) {
            List<String> params = this.uri.getPathParameters().get(key);
            E.checkState(params != null && params.size() == 1,
                         "There is no matched path parameter: '%s'", key);
            return params.get(0);
        }

        private final class UserPrincipal implements Principal {

            @Override
            public String getName() {
                return Authorizer.this.user.getName();
            }

            @Override
            public String toString() {
                return Authorizer.this.user.toString();
            }

            @Override
            public int hashCode() {
                return Authorizer.this.user.hashCode();
            }

            @Override
            public boolean equals(Object obj) {
                return Authorizer.this.user.equals(obj);
            }
        }
    }

    public static boolean isWhiteAPI(ContainerRequestContext context) {
        String path = context.getUriInfo().getPath();

        E.checkArgument(StringUtils.isNotEmpty(path),
                        "Invalid request uri '%s'", path);

        for (String whiteApi : WHITE_API_LIST) {
            if (path.endsWith(whiteApi)) {
                return true;
            }
        }
        return false;
    }
}
