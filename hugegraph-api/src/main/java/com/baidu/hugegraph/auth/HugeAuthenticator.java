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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.tinkerpop.gremlin.groovy.jsr223.dsl.credential.CredentialGraphTokens;
import org.apache.tinkerpop.gremlin.server.auth.AuthenticatedUser;
import org.apache.tinkerpop.gremlin.server.auth.AuthenticationException;
import org.apache.tinkerpop.gremlin.server.auth.Authenticator;
import org.apache.tinkerpop.shaded.jackson.annotation.JsonProperty;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.auth.HugeGraphAuthProxy.Context;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.config.OptionSpace;
import com.baidu.hugegraph.config.ServerOptions;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.JsonUtil;

public interface HugeAuthenticator extends Authenticator {

    public static final String KEY_USERNAME =
                               CredentialGraphTokens.PROPERTY_USERNAME;
    public static final String KEY_PASSWORD =
                               CredentialGraphTokens.PROPERTY_PASSWORD;

    public static final String ROLE_NONE = "";
    public static final String ROLE_ADMIN = "admin";
    public static final String ROLE_USER = "user";
    public static final String ROLE_OWNER = "$owner";
    public static final String ROLE_DYNAMIC = "$dynamic";

    public static final String ACTION = "$action";

    public void setup(HugeConfig config);

    public String authenticate(String username, String password);

    @Override
    public default void setup(final Map<String, Object> config) {
        E.checkState(config != null,
                     "Must provide a 'config' in the 'authentication'");
        String path = (String) config.get("tokens");
        E.checkState(path != null,
                     "Credentials configuration missing key 'tokens'");
        OptionSpace.register("tokens", ServerOptions.instance());
        this.setup(new HugeConfig(path));
    }

    @Override
    public default User authenticate(final Map<String, String> credentials)
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

    @Override
    public default boolean requireAuthentication() {
        return true;
    }

    public default boolean verifyRole(String role) {
        if (role == ROLE_NONE || role == null || role.isEmpty()) {
            return false;
        } else {
            return true;
        }
    }

    public static class User extends AuthenticatedUser {

        protected static final String USER_ADMIN = ROLE_ADMIN;
        protected static final String USER_ANONY = ANONYMOUS_USERNAME;

        public static final User ADMIN = new User(USER_ADMIN, ROLE_ADMIN);
        public static final User ANONYMOUS = new User(USER_ANONY, ROLE_ADMIN);

        private final String role;

        public User(String username, String role) {
            super(username);
            this.role = role;
        }

        public String username() {
            return this.getName();
        }

        public String role() {
            return this.role;
        }

        @Override
        public boolean isAnonymous() {
            return this == ANONYMOUS || this == ANONYMOUS_USER;
        }

        @Override
        public int hashCode() {
            return this.username().hashCode() ^ this.role().hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof User)) {
                return false;
            }

            User other = (User) obj;
            return this.username().equals(other.username()) &&
                   this.role().equals(other.role());
        }

        @Override
        public String toString() {
            return String.format("User{username=%s,role=%s}",
                                 this.username(), this.role());
        }
    }

    public static class RoleAction {

        @JsonProperty("owners")
        private Set<String> owners;
        @JsonProperty("actions")
        private Set<String> actions;

        public RoleAction() {
            this.owners = new HashSet<>();
            this.actions = new HashSet<>();
        }

        public RoleAction owner(String... owners) {
            this.owners.addAll(Arrays.asList(owners));
            return this;
        }

        public Set<String> owners() {
            return Collections.unmodifiableSet(this.owners);
        }

        public String owner() {
            if (owners.size() == 1) {
                return owners.iterator().next();
            }
            return null;
        }

        public RoleAction action(String... actions) {
            this.actions.addAll(Arrays.asList(actions));
            return this;
        }

        public Set<String> actions() {
            return Collections.unmodifiableSet(this.actions);
        }

        public boolean matchOwner(String owner) {
            if (owner == null) {
                return true;
            }
            return this.owners.contains(owner);
        }

        public boolean matchAction(String action) {
            if (action == null) {
                return true;
            }
            return this.actions.contains(action);
        }

        public boolean matchAction(Set<String> actions) {
            return this.actions.containsAll(actions);
        }

        public String toRole() {
            return JsonUtil.toJson(this);
        }

        @Override
        public String toString() {
            return this.toRole();
        }

        public static String ownerFor(String name) {
            return ROLE_OWNER + "=" + name;
        }

        public static RoleAction fromRole(String role) {
            return JsonUtil.fromJson(role, RoleAction.class);
        }

        public static RoleAction fromPermission(String permission) {
            RoleAction roleAction = new RoleAction();
            String[] ownerAction = permission.split(" ");
            String[] ownerKV = ownerAction[0].split("=", 2);
            E.checkState(ownerKV.length == 2 && ownerKV[0].equals(ROLE_OWNER),
                         "Bad permission format: '%s'", permission);
            roleAction.owners.add(ownerKV[1]);
            if (ownerAction.length == 1) {
                // no action
                return roleAction;
            }

            E.checkState(ownerAction.length == 2,
                         "Bad permission format: '%s'", permission);
            String[] actionKV = ownerAction[1].split("=", 2);
            E.checkState(actionKV.length == 2,
                         "Bad permission format: '%s'", permission);
            E.checkState(actionKV[0].equals(StandardAuthenticator.ACTION),
                         "Bad permission format: '%s'", permission);
            roleAction.actions.add(actionKV[1]);

            return roleAction;
        }

        public static boolean match(String role, String permission) {
            RoleAction rolePermission = RoleAction.fromPermission(permission);
            RoleAction roleAction = RoleAction.fromRole(role);

            if (!roleAction.matchOwner(rolePermission.owner())) {
                return false;
            }
            return roleAction.matchAction(rolePermission.actions());
        }
    }

    public static HugeAuthenticator loadAuthenticator(HugeConfig conf) {
        String authClass = conf.get(ServerOptions.AUTHENTICATOR);
        if (authClass.isEmpty()) {
            return null;
        }

        HugeAuthenticator authenticator;
        ClassLoader cl = conf.getClass().getClassLoader();
        try {
            authenticator = (HugeAuthenticator) cl.loadClass(authClass)
                                                  .newInstance();
        } catch (Exception e) {
            throw new HugeException("Failed to load authenticator: '%s'",
                                    authClass, e);
        }

        authenticator.setup(conf);

        return authenticator;
    }
}
