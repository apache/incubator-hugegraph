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

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.NotImplementedException;
import org.apache.tinkerpop.gremlin.groovy.jsr223.dsl.credential.CredentialGraphTokens;
import org.apache.tinkerpop.gremlin.server.auth.AuthenticationException;

import com.baidu.hugegraph.auth.HugeGraphAuthProxy.Context;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.config.OptionSpace;
import com.baidu.hugegraph.config.ServerOptions;
import com.baidu.hugegraph.util.E;

public class StandardAuthenticator implements HugeAuthenticator {

    public static final String KEY_USERNAME =
                               CredentialGraphTokens.PROPERTY_USERNAME;
    public static final String KEY_PASSWORD =
                               CredentialGraphTokens.PROPERTY_PASSWORD;

    private final Map<String, String> tokens;

    public StandardAuthenticator() {
        this.tokens = new HashMap<>();
    }

    @Override
    public void setup(HugeConfig config) {
        this.tokens.put(User.USER_ADMIN, config.get(ServerOptions.ADMIN_TOKEN));
        this.tokens.putAll(config.getMap(ServerOptions.USER_TOKENS));
    }

    @Override
    public void setup(final Map<String, Object> config) {
        E.checkState(config != null,
                     "Must provide a 'config' in the 'authentication'");
        String path = (String) config.get("tokens");
        E.checkState(path != null,
                     "Credentials configuration missing key 'tokens'");
        OptionSpace.register("tokens", ServerOptions.instance());
        this.setup(new HugeConfig(path));
    }

    @Override
    public User authenticate(final Map<String, String> credentials)
                             throws AuthenticationException {
        User user = User.ANONYMOUS;
        if (this.requireAuthentication()) {
            String username = credentials.get(KEY_USERNAME);
            String password = credentials.get(KEY_PASSWORD);

            // Currently we just use config tokens to authenticate
            String role = this.authenticate(username, password);
            if (!verifyRole(role)) {
                // Throw if not certified
                String message = "Incorrect username or password";
                throw new AuthenticationException(message);
            }
            user = new User(username, role);
        }
        /*
         * Set authentication context
         * TODO: unset context after finishing a request
         */
        HugeGraphAuthProxy.setContext(new Context(user));

        return user;
    }

    /**
     * Verify if a user is legal
     * @param username  the username for authentication
     * @param password  the password for authentication
     * @return String No permission if return ROLE_NONE else return a role
     */
    public String authenticate(final String username, final String password) {
        E.checkArgumentNotNull(username,
                               "The username parameter can't be null");
        E.checkArgumentNotNull(password,
                               "The password parameter can't be null");

        String role;
        if (password.equals(this.tokens.get(username))) {
            // Return user name as role
            role = username;
        } else {
            role = ROLE_NONE;
        }

        return role;
    }

    @Override
    public SaslNegotiator newSaslNegotiator() {
        throw new NotImplementedException("SaslNegotiator is unsupported");
    }

    public static final boolean verifyRole(String role) {
        if (role == ROLE_NONE || role == null || role.isEmpty()) {
            return false;
        } else {
            return true;
        }
    }
}
