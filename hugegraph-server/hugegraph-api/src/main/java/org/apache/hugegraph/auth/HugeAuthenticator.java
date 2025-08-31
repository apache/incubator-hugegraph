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

package org.apache.hugegraph.auth;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hugegraph.HugeException;
import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.auth.HugeGraphAuthProxy.Context;
import org.apache.hugegraph.auth.SchemaDefine.AuthElement;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.id.IdGenerator;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.config.OptionSpace;
import org.apache.hugegraph.config.ServerOptions;
import org.apache.hugegraph.type.Nameable;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.JsonUtil;
import org.apache.tinkerpop.gremlin.groovy.jsr223.dsl.credential.CredentialGraphTokens;
import org.apache.tinkerpop.gremlin.server.auth.AuthenticatedUser;
import org.apache.tinkerpop.gremlin.server.auth.AuthenticationException;
import org.apache.tinkerpop.gremlin.server.auth.Authenticator;
import org.apache.tinkerpop.shaded.jackson.annotation.JsonProperty;

import jakarta.ws.rs.core.SecurityContext;

public interface HugeAuthenticator extends Authenticator {

    String KEY_USERNAME = CredentialGraphTokens.PROPERTY_USERNAME;
    String KEY_PASSWORD = CredentialGraphTokens.PROPERTY_PASSWORD;
    String KEY_TOKEN = "token";
    String KEY_ROLE = "role";
    String KEY_ADDRESS = "address";
    String KEY_PATH = "path";

    String USER_SYSTEM = "system";
    String USER_ADMIN = "admin";
    String USER_ANONY = AuthenticatedUser.ANONYMOUS_USERNAME;

    RolePermission ROLE_NONE = RolePermission.none();
    RolePermission ROLE_ADMIN = RolePermission.admin();

    String VAR_PREFIX = "$";
    String KEY_GRAPHSPACE = VAR_PREFIX + "graphspace";
    String KEY_OWNER = VAR_PREFIX + "owner";
    String KEY_DYNAMIC = VAR_PREFIX + "dynamic";
    String KEY_ACTION = VAR_PREFIX + "action";

    void setup(HugeConfig config);

    UserWithRole authenticate(String username, String password, String token);

    void unauthorize(SecurityContext context);

    AuthManager authManager();

    HugeGraph graph();

    @Override
    default void setup(final Map<String, Object> config) {
        E.checkState(config != null,
                     "Must provide a 'config' in the 'authentication'");
        String path = (String) config.get("tokens");
        E.checkState(path != null,
                     "Credentials configuration missing key 'tokens'");
        OptionSpace.register("tokens", ServerOptions.instance());
        this.setup(new HugeConfig(path));
    }

    @Override
    default User authenticate(final Map<String, String> credentials)
            throws AuthenticationException {

        HugeGraphAuthProxy.resetContext();

        User user = User.ANONYMOUS;
        if (this.requireAuthentication()) {
            String username = credentials.get(KEY_USERNAME);
            String password = credentials.get(KEY_PASSWORD);
            String token = credentials.get(KEY_TOKEN);

            // Currently we just use config tokens to authenticate
            UserWithRole role = this.authenticate(username, password, token);
            if (!verifyRole(role.role())) {
                // Throw if not certified
                String message = "Incorrect username or password";
                throw new AuthenticationException(message);
            }
            user = new User(role.username(), role.role());
            user.client(credentials.get(KEY_ADDRESS));
        }

        HugeGraphAuthProxy.logUser(user, credentials.get(KEY_PATH));
        // TODO: Ensure context lifecycle in GraphServer & AuthServer(#AccessLogFilter)
        HugeGraphAuthProxy.setContext(new Context(user));

        return user;
    }

    @Override
    default boolean requireAuthentication() {
        return true;
    }

    default boolean verifyRole(RolePermission role) {
        return role != ROLE_NONE && role != null;
    }

    void initAdminUser(String password) throws Exception;

    static HugeAuthenticator loadAuthenticator(HugeConfig conf) {
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

    class User extends AuthenticatedUser {

        public static final User ADMIN = new User(USER_ADMIN, ROLE_ADMIN);
        public static final User ANONYMOUS = new User(USER_ANONY, ROLE_ADMIN);

        private final RolePermission role;
        private final Id userId;
        private String client; // peer

        public User(String username, RolePermission role) {
            super(username);
            E.checkNotNull(username, "username");
            E.checkNotNull(role, "role");
            this.role = role;
            this.client = null;
            /*
             * 1. Use username as the id to simplify getting userId
             * 2. Only used as cache's key in auth proxy now
             */
            this.userId = IdGenerator.of(username);
        }

        public String username() {
            return this.getName();
        }

        public Id userId() {
            return this.userId;
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

    class RolePerm {

        @JsonProperty("roles") // graph -> action -> resource
        private final Map<String, Map<HugePermission, Object>> roles;

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

        private boolean matchResource(HugePermission requiredAction,
                                      ResourceObject<?> requiredResource) {
            E.checkNotNull(requiredResource, "resource object");

            /*
             * Is resource allowed to access by anyone?
             * TODO: only allowed resource of related type(USER/TASK/VAR),
             *       such as role VAR is allowed to access '~variables' label
             */
            if (HugeResource.allowed(requiredResource)) {
                return true;
            }

            String owner = requiredResource.graph();
            Map<HugePermission, Object> permissions = this.roles.get(owner);
            if (permissions == null) {
                return false;
            }
            Object permission = matchedAction(requiredAction, permissions);
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
                if (res.filter(requiredResource)) {
                    return true;
                }
            }
            return false;
        }

        private static Object matchedAction(HugePermission action,
                                            Map<HugePermission, Object> perms) {
            Object matched = perms.get(action);
            if (matched != null) {
                return matched;
            }
            for (Map.Entry<HugePermission, Object> e : perms.entrySet()) {
                HugePermission permission = e.getKey();
                // May be required = ANY
                if (action.match(permission)) {
                    // Return matched resource of corresponding action
                    return e.getValue();
                }
            }
            return null;
        }

        @SuppressWarnings({"unchecked", "rawtypes"})
        public static RolePerm fromJson(Object role) {
            RolePermission table = RolePermission.fromJson(role);
            return new RolePerm((Map) table.map());
        }

        public static boolean match(Object role, RequiredPerm requiredPerm) {
            if (role == ROLE_ADMIN) {
                return true;
            }
            if (role == ROLE_NONE) {
                return false;
            }

            RolePerm rolePerm = RolePerm.fromJson(role);

            if (requiredPerm.action() == HugePermission.NONE) {
                // None action means any action is OK if the owner matched
                return rolePerm.matchOwner(requiredPerm.owner());
            }
            return rolePerm.matchResource(requiredPerm.action(),
                                          requiredPerm.resourceObject());
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
                AuthElement element = (AuthElement) resourceObject.operated();
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

    class RequiredPerm {

        @JsonProperty("owner")
        private String owner;
        @JsonProperty("action")
        private HugePermission action;
        @JsonProperty("resource")
        private ResourceType resource;

        public RequiredPerm() {
            this.owner = "";
            this.action = HugePermission.NONE;
            this.resource = ResourceType.NONE;
        }

        public RequiredPerm owner(String owner) {
            this.owner = owner;
            return this;
        }

        public String owner() {
            return this.owner;
        }

        public RequiredPerm action(String action) {
            this.parseAction(action);
            return this;
        }

        public HugePermission action() {
            return this.action;
        }

        public ResourceType resource() {
            return this.resource;
        }

        public ResourceObject<?> resourceObject() {
            Nameable elem = HugeResource.NameObject.ANY;
            return ResourceObject.of(this.owner, this.resource, elem);
        }

        @Override
        public String toString() {
            return JsonUtil.toJson(this);
        }

        private void parseAction(String action) {
            int offset = action.lastIndexOf('_');
            if (0 < offset && ++offset < action.length()) {
                /*
                 * In order to be compatible with the old permission mechanism,
                 * here is only to provide pre-control by extract the
                 * resource_action {vertex/edge/schema}_{read/write},
                 * resource_action like vertex_read.
                 */
                String resource = action.substring(0, offset - 1);
                this.resource = ResourceType.valueOf(resource.toUpperCase());
                action = action.substring(offset);
            }
            this.action = HugePermission.valueOf(action.toUpperCase());
        }

        public static String roleFor(String graphSpace, String owner,
                                     HugePermission perm) {
            /*
             * Construct required permission such as:
             *  $owner=graph1 $action=read
             *  (means required read permission of any one resource)
             *
             * In the future maybe also support:
             *  $owner=graph1 $action=vertex_read
             */
            return String.format("%s=%s %s=%s %s=%s",
                                 KEY_GRAPHSPACE, graphSpace,
                                 KEY_OWNER, owner,
                                 KEY_ACTION, perm.string());
        }
        public static RequiredPerm fromJson(String json) {
            return JsonUtil.fromJson(json, RequiredPerm.class);
        }

        public static RequiredPerm fromPermission(String permission) {
            // Permission format like: "$owner=$graph1 $action=vertex-write"
            RequiredPerm requiredPerm = new RequiredPerm();
            String[] ownerAndAction = permission.split(" ");
            String[] ownerKV = ownerAndAction[0].split("=", 2);
            E.checkState(ownerKV.length == 2 && ownerKV[0].equals(KEY_OWNER),
                         "Bad permission format: '%s'", permission);
            requiredPerm.owner(ownerKV[1]);
            if (ownerAndAction.length == 1) {
                // Return owner if no action (means NONE)
                return requiredPerm;
            }

            E.checkState(ownerAndAction.length == 2,
                         "Bad permission format: '%s'", permission);
            String[] actionKV = ownerAndAction[1].split("=", 2);
            E.checkState(actionKV.length == 2,
                         "Bad permission format: '%s'", permission);
            E.checkState(actionKV[0].equals(KEY_ACTION),
                         "Bad permission format: '%s'", permission);
            requiredPerm.action(actionKV[1]);

            return requiredPerm;
        }
    }
}
