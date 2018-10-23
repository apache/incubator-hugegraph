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

package com.baidu.hugegraph.auth;

import static org.apache.tinkerpop.gremlin.groovy.jsr223.dsl.credential.CredentialGraphTokens.PROPERTY_PASSWORD;
import static org.apache.tinkerpop.gremlin.groovy.jsr223.dsl.credential.CredentialGraphTokens.PROPERTY_USERNAME;

import java.util.HashMap;
import java.util.Map;

import org.apache.tinkerpop.gremlin.server.auth.AuthenticatedUser;
import org.apache.tinkerpop.gremlin.server.auth.AuthenticationException;
import org.apache.tinkerpop.gremlin.server.auth.SimpleAuthenticator;

import com.baidu.hugegraph.auth.HugeGraphAuthProxy.Context;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.config.OptionSpace;
import com.baidu.hugegraph.config.ServerOptions;
import com.baidu.hugegraph.util.E;

public class StandardAuthenticator extends SimpleAuthenticator {

    public static final String ROLE_NONE = "";
    public static final String ROLE_ADMIN = "admin";
    public static final String ROLE_USER = "user";
    public static final String ROLE_OWNER = "$owner";
    public static final String ROLE_DYNAMIC = "$dynamic";

    public static final String USER_ADMIN = ROLE_ADMIN;
    public static final String USER_ANONYMOUS =
                               AuthenticatedUser.ANONYMOUS_USERNAME;

    private boolean requireAuth;
    private final Map<String, String> tokens;

    public StandardAuthenticator(HugeConfig config) {
        this();
        this.load(config);
    }

    public StandardAuthenticator() {
        this.requireAuth = true;
        this.tokens = new HashMap<>();
    }

    @Override
    public void setup(final Map<String, Object> config) {
        E.checkState(config != null,
                     "Must provide a 'config' in the 'authentication'");
        String path = (String) config.get("tokens");
        E.checkState(path != null,
                     "Credentials configuration missing key 'tokens'");
        OptionSpace.register("tokens", ServerOptions.instance());
        this.load(new HugeConfig(path));
    }

    @Override
    public boolean requireAuthentication() {
        return this.requireAuth;
    }

    /**
     * This authentication method is for Gremlin Server
     */
    @Override
    public AuthenticatedUser authenticate(final Map<String, String> credentials)
                                          throws AuthenticationException {
        if (!this.requireAuthentication()) {
            return AuthenticatedUser.ANONYMOUS_USER;
        }
        String username = credentials.get(PROPERTY_USERNAME);
        String password = credentials.get(PROPERTY_PASSWORD);

        // Currently we just use config tokens to authenticate
        String role = this.authenticate(username, password);
        if (!verifyRole(role)) {
            throw new AuthenticationException("Invalid username or password");
        }

        return new AuthenticatedUser(username);
    }

    /**
     * Verify if a user is legal
     * @param username
     * @param password
     * @return String No permission if return ROLE_NONE else return a role
     */
    public String authenticate(final String username, final String password) {
        E.checkArgumentNotNull(username, "The username can't be null");
        E.checkArgumentNotNull(password, "The password can't be null");

        String role;
        if (password.equals(this.tokens.get(username))) {
            // Return user name as role
            role = username;
        } else {
            role = ROLE_NONE;
        }

        /*
         * Set authentication context
         * TODO: unset context after finishing a request,
         * now must set context to update last context even if not authorized.
         */
        HugeGraphAuthProxy.setContext(new Context(username, role));

        return role;
    }

    private void load(HugeConfig config) {
        this.requireAuth = config.get(ServerOptions.REQUIRE_AUTH);
        this.tokens.put(USER_ADMIN, config.get(ServerOptions.ADMIN_TOKEN));
        this.tokens.putAll(config.getMap(ServerOptions.USER_TOKENS));
    }

    public static final boolean verifyRole(String role) {
        if (role == ROLE_NONE || role == null || role.isEmpty()) {
            return false;
        } else {
            return true;
        }
    }
}
