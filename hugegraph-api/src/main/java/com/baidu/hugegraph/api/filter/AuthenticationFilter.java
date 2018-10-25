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
import java.util.List;

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

import org.apache.tinkerpop.gremlin.server.auth.AuthenticationException;
import org.glassfish.grizzly.utils.Charsets;

import com.baidu.hugegraph.auth.HugeAuthenticator.User;
import com.baidu.hugegraph.auth.StandardAuthenticator;
import com.baidu.hugegraph.core.GraphManager;
import com.baidu.hugegraph.util.E;
import com.google.common.collect.ImmutableMap;

@Provider
@PreMatching
@Priority(Priorities.AUTHENTICATION)
public class AuthenticationFilter implements ContainerRequestFilter {

    @Context
    private javax.inject.Provider<GraphManager> managerProvider;

    @Override
    public void filter(ContainerRequestContext context) throws IOException {
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

        // Extract authentication credentials
        String auth = context.getHeaderString(HttpHeaders.AUTHORIZATION);
        if (auth == null) {
            throw new NotAuthorizedException(
                      "Authentication credentials are required",
                      "Missing authentication credentials");
        }
        if (!auth.startsWith("Basic ")) {
            throw new BadRequestException(
                      "Only HTTP Basic authentication is supported");
        }

        auth = auth.substring("Basic ".length());
        auth = new String(DatatypeConverter.parseBase64Binary(auth),
                          Charsets.ASCII_CHARSET);
        String[] values = auth.split(":");
        if (values.length != 2) {
            throw new BadRequestException(
                      "Invalid syntax for username and password");
        }

        final String username = values[0];
        final String password = values[1];
        assert username != null && password != null;

        // Validate the extracted credentials
        try {
            return manager.authenticate(ImmutableMap.of(
                           StandardAuthenticator.KEY_USERNAME, username,
                           StandardAuthenticator.KEY_PASSWORD, password));
        } catch (AuthenticationException e) {
            String msg = String.format("Authentication failed for user '%s'",
                                       username);
            throw new NotAuthorizedException(msg, e.getMessage());
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

        public String role() {
            return this.user.role();
        }

        @Override
        public Principal getUserPrincipal() {
            return this.principal;
        }

        @Override
        public boolean isUserInRole(String role) {
            if (role.equals(StandardAuthenticator.ROLE_DYNAMIC)) {
                // Let the resource itself dynamically determine
                return true;
            } else if (role.startsWith(StandardAuthenticator.ROLE_OWNER)) {
                // Role format like: "$owner=name"
                String[] owner = role.split("=", 2);
                E.checkState(owner.length == 2, "Bad role format: '%s'", role);
                role = this.getPathParameter(owner[1]);
            }
            return role.equals(this.user.role());
        }

        @Override
        public boolean isSecure() {
            return "https".equals(this.uri.getRequestUri().getScheme());
        }

        @Override
        public String getAuthenticationScheme() {
            return SecurityContext.BASIC_AUTH;
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
                return Authorizer.this.user.role();
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
}
