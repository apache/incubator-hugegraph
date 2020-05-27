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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.tinkerpop.gremlin.groovy.jsr223.dsl.credential.CredentialGraphTokens;
import org.apache.tinkerpop.gremlin.server.auth.AuthenticatedUser;
import org.apache.tinkerpop.gremlin.server.auth.AuthenticationException;
import org.apache.tinkerpop.gremlin.server.auth.Authenticator;
import org.apache.tinkerpop.shaded.jackson.annotation.JsonProperty;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.auth.HugeGraphAuthProxy.Context;
import com.baidu.hugegraph.auth.SchemaDefine.UserElement;
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
    public static final String KEY_ROLE = "role";
    public static final String KEY_CLIENT = "address";
    public static final String KEY_PATH = "path";

    public static final String USER_SYSTEM = "system";
    public static final String USER_ADMIN = "admin";
    public static final String USER_ANONY = AuthenticatedUser.ANONYMOUS_USERNAME;

    public static final RolePermission ROLE_NONE = RolePermission.none();
    public static final RolePermission ROLE_ADMIN = RolePermission.admin();

    public static final String VAR_PREFIX = "$";
    public static final String KEY_OWNER = VAR_PREFIX + "owner";
    public static final String KEY_DYNAMIC = VAR_PREFIX + "dynamic";
    public static final String KEY_ACTION = VAR_PREFIX + "action";

    public void setup(HugeConfig config);

    public RolePermission authenticate(String username, String password);
    public UserManager userManager();

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
        HugeGraphAuthProxy.resetContext();

        User user = User.ANONYMOUS;
        if (this.requireAuthentication()) {
            String username = credentials.get(KEY_USERNAME);
            String password = credentials.get(KEY_PASSWORD);

            // Currently we just use config tokens to authenticate
            RolePermission role = this.authenticate(username, password);
            if (!verifyRole(role)) {
                // Throw if not certified
                String message = "Incorrect username or password";
                throw new AuthenticationException(message);
            }
            user = new User(username, role);
            user.client(credentials.get(KEY_CLIENT));
        }

        HugeGraphAuthProxy.logUser(user, credentials.get(KEY_PATH));
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

    public default boolean verifyRole(RolePermission role) {
        if (role == ROLE_NONE || role == null) {
            return false;
        } else {
            return true;
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

    public static class User extends AuthenticatedUser {

        public static final User ADMIN = new User(USER_ADMIN, ROLE_ADMIN);
        public static final User ANONYMOUS = new User(USER_ANONY, ROLE_ADMIN);

        private final RolePermission role;
        private String client; // peer

        public User(String username, RolePermission role) {
            super(username);
            E.checkNotNull(username, "username");
            E.checkNotNull(role, "role");
            this.role = role;
            this.client = null;
        }

        public String username() {
            return this.getName();
        }

        public RolePermission role() {
            return this.role;
        }

        public void client(String client) {
            this.client = client;
        }

        public String client() {
            return client;
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
        public boolean equals(Object object) {
            if (this == object) {
                return true;
            }
            if (!(object instanceof User)) {
                return false;
            }

            User other = (User) object;
            return this.username().equals(other.username()) &&
                   this.role().equals(other.role());
        }

        @Override
        public String toString() {
            return String.format("User{username=%s,role=%s}",
                                 this.username(), this.role());
        }

        public String toJson() {
            UserJson json = new UserJson();
            json.username = this.username();
            json.role = this.role();
            json.client = this.client();
            return JsonUtil.toJson(json);
        }

        public static User fromJson(String json) {
            if (json == null) {
                return null;
            }
            UserJson userJson = JsonUtil.fromJson(json, UserJson.class);
            if (userJson != null) {
                User user = new User(userJson.username,
                                     RolePermission.builtin(userJson.role));
                user.client(userJson.client);
                return user;
            }
            return null;
        }

        public static class UserJson {

            @JsonProperty("username")
            private String username;
            @JsonProperty("role")
            private RolePermission role;
            @JsonProperty("client")
            private String client;
        }
    }

    public static class RolePerm {

        @JsonProperty("roles") // graph -> action -> resource
        private Map<String, Map<HugePermission, Object>> roles;

        public RolePerm() {
            this.roles = new HashMap<>();
        }

        public RolePerm(Map<String, Map<HugePermission, Object>> roles) {
            this.roles = roles;
        }

        @Override
        public String toString() {
            return JsonUtil.toJson(this);
        }

        private boolean matchOwner(String owner) {
            if (owner == null) {
                return true;
            }
            return this.roles.containsKey(owner);
        }

        private boolean matchPermission(String owner,
                                        Set<HugePermission> actions) {
            // It's OK if owner and action are matched
            if (owner == null) {
                return true;
            }
            Map<HugePermission, Object> permissions = this.roles.get(owner);
            if (permissions == null) {
                return false;
            }
            if (permissions.keySet().containsAll(actions)) {
                // All actions are matched (string equal)
                return true;
            }
            for (HugePermission action : actions) {
                if (!this.matchAction(action, permissions)) {
                    // Permission denied for `action`
                    return false;
                }
            }
            return true;
        }

        private boolean matchResource(HugePermission required,
                                      ResourceObject<?> resourceObject) {
            E.checkNotNull(resourceObject, "resource object");

            /*
             * Is resource allowed to access by anyone?
             * TODO: only allowed resource of related type(USER/TASK/VAR),
             *       such as role VAR is allowed to access '~variables' label
             */
            if (HugeResource.allowed(resourceObject)) {
                return true;
            }

            String owner = resourceObject.graph();
            Map<HugePermission, Object> permissions = this.roles.get(owner);
            if (permissions == null) {
                return false;
            }
            Object permission = matchedAction(required, permissions);
            if (permission == null) {
                // Deny all if no specified permission
                return false;
            }
            List<HugeResource> ress;
            if (permission instanceof List) {
                @SuppressWarnings("unchecked")
                List<HugeResource> list = (List<HugeResource>) permission;
                ress = list;
            } else {
                ress = HugeResource.parseResources(permission.toString());
            }
            for (HugeResource res : ress) {
                if (res.filter(resourceObject)) {
                    return true;
                }
            }
            return false;
        }

        private boolean matchAction(HugePermission required,
                                    Map<HugePermission, Object> permissions) {
            if (required == null || permissions.containsKey(required)) {
                return true;
            }
            return matchedAction(required, permissions) != null;
        }

        private Object matchedAction(HugePermission required,
                                     Map<HugePermission, Object> permissions) {
            Object matched = permissions.get(required);
            if (matched != null) {
                return matched;
            }
            for (Map.Entry<HugePermission, Object> e : permissions.entrySet()) {
                HugePermission permission = e.getKey();
                if (required.match(permission)) {
                    return e.getValue();
                }
            }
            return null;
        }

        @SuppressWarnings({ "unchecked", "rawtypes" })
        public static RolePerm fromJson(Object role) {
            RolePermission table = RolePermission.fromJson(role);
            return new RolePerm((Map) table.map());
        }

        public static boolean match(Object role, Object requiredPerm) {
            if (role == ROLE_ADMIN) {
                return true;
            }
            if (role == ROLE_NONE) {
                return false;
            }
            RolePerm rolePerm = RolePerm.fromJson(role);
            RoleAction roleAction;
            if (requiredPerm instanceof RoleAction) {
                roleAction = (RoleAction) requiredPerm;
            } else {
                // The required like: $owner=graph1 $action=vertex-write
                String required = (String) requiredPerm;
                if (!required.startsWith(KEY_OWNER)) {
                    /*
                     * The required parameter means the owner if not started
                     * with ROLE_OWNER, any action is OK if the owner matched.
                     */
                    return rolePerm.matchOwner(required);
                }
                roleAction = RoleAction.fromPermission(required);
            }
            return rolePerm.matchPermission(roleAction.owner(),
                                            roleAction.actions());
        }

        public static boolean match(Object role, HugePermission required,
                                    ResourceObject<?> resourceObject) {
            if (role == ROLE_ADMIN) {
                return true;
            }
            if (role == ROLE_NONE) {
                return false;
            }
            RolePerm rolePerm = RolePerm.fromJson(role);
            return rolePerm.matchResource(required, resourceObject);
        }

        public static boolean match(Object role, RolePermission grant,
                                    ResourceObject<?> resourceObject) {
            if (role == ROLE_ADMIN) {
                return true;
            }
            if (role == ROLE_NONE) {
                return false;
            }

            if (resourceObject != null) {
                UserElement element = (UserElement) resourceObject.operated();
                if (element instanceof HugeUser &&
                    ((HugeUser) element).name().equals(USER_ADMIN)) {
                    // Can't access admin by other users
                    return false;
                }
            }

            RolePermission rolePerm = RolePermission.fromJson(role);
            return rolePerm.contains(grant);
        }
    }

    public static class RoleAction {

        @JsonProperty("owner")
        private String owner;
        @JsonProperty("actions")
        private Set<HugePermission> actions;

        public RoleAction() {
            this.owner = "";
            this.actions = new HashSet<>();
        }

        public RoleAction owner(String owner) {
            this.owner = owner;
            return this;
        }

        public String owner() {
            return this.owner;
        }

        public RoleAction action(String... actions) {
            for (String action : actions) {
                this.actions.add(parseAction(action));
            }
            return this;
        }

        public Set<HugePermission> actions() {
            return Collections.unmodifiableSet(this.actions);
        }

        @Override
        public String toString() {
            return JsonUtil.toJson(this);
        }

        private HugePermission parseAction(String action) {
            int offset = action.lastIndexOf('_');
            if (0 < offset && ++offset < action.length()) {
                /*
                 * In order to be compatible with the old permission mechanism,
                 * here is only to provide pre-control by extract the suffix of
                 * action {vertex/edge/schema}_{read/write} like vertex_read.
                 */
                action = action.substring(offset);
            }
            return HugePermission.valueOf(action.toUpperCase());
        }

        public static String roleFor(String owner) {
            return KEY_OWNER + "=" + owner;
        }

        public static RoleAction fromJson(String json) {
            return JsonUtil.fromJson(json, RoleAction.class);
        }

        public static RoleAction fromPermission(String permission) {
            // Permission format like: "$owner=$graph1 $action=vertex-write"
            RoleAction roleAction = new RoleAction();
            String[] ownerAndAction = permission.split(" ");
            String[] ownerKV = ownerAndAction[0].split("=", 2);
            E.checkState(ownerKV.length == 2 && ownerKV[0].equals(KEY_OWNER),
                         "Bad permission format: '%s'", permission);
            roleAction.owner(ownerKV[1]);
            if (ownerAndAction.length == 1) {
                // Return owner if no action
                return roleAction;
            }

            E.checkState(ownerAndAction.length == 2,
                         "Bad permission format: '%s'", permission);
            String[] actionKV = ownerAndAction[1].split("=", 2);
            E.checkState(actionKV.length == 2,
                         "Bad permission format: '%s'", permission);
            E.checkState(actionKV[0].equals(StandardAuthenticator.KEY_ACTION),
                         "Bad permission format: '%s'", permission);
            roleAction.action(actionKV[1]);

            return roleAction;
        }
    }
}
