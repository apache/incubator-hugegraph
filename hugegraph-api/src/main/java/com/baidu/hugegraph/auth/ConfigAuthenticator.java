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

import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.NotImplementedException;
import org.apache.tinkerpop.gremlin.groovy.jsr223.dsl.credential.CredentialGraphTokens;

import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.config.ServerOptions;
import com.baidu.hugegraph.util.E;

public class ConfigAuthenticator implements HugeAuthenticator {

    public static final String KEY_USERNAME =
                               CredentialGraphTokens.PROPERTY_USERNAME;
    public static final String KEY_PASSWORD =
                               CredentialGraphTokens.PROPERTY_PASSWORD;

    private final Map<String, String> tokens;

    public ConfigAuthenticator() {
        this.tokens = new HashMap<>();
    }

    @Override
    public void setup(HugeConfig config) {
        this.tokens.putAll(config.getMap(ServerOptions.AUTH_USER_TOKENS));
        assert !this.tokens.containsKey(USER_ADMIN);
        this.tokens.put(USER_ADMIN, config.get(ServerOptions.AUTH_ADMIN_TOKEN));
    }

    /**
     * Verify if a user is legal
     * @param username  the username for authentication
     * @param password  the password for authentication
     * @return String No permission if return ROLE_NONE else return a role
     */
    @Override
    public UserWithRole authenticate(final String username,
                                     final String password,
                                     final String token) {
        E.checkArgumentNotNull(username,
                               "The username parameter can't be null");
        E.checkArgumentNotNull(password,
                               "The password parameter can't be null");
        E.checkArgument(token == null, "The token must be null");

        RolePermission role;
        if (password.equals(this.tokens.get(username))) {
            if (username.equals(USER_ADMIN)) {
                role = ROLE_ADMIN;
            } else {
                // Return role with all permission, set user name as owner graph
                role = RolePermission.all(username);
            }
        } else {
            role = ROLE_NONE;
        }

        return new UserWithRole(IdGenerator.of(username), username, role);
    }

    @Override
    public AuthManager authManager() {
        throw new NotImplementedException(
                  "AuthManager is unsupported by ConfigAuthenticator");
    }

    @Override
    public SaslNegotiator newSaslNegotiator(InetAddress remoteAddress) {
        throw new NotImplementedException(
                  "SaslNegotiator is unsupported by ConfigAuthenticator");
    }
}
