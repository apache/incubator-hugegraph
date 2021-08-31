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

import java.io.Console;
import java.net.InetAddress;
import java.util.Scanner;

import com.baidu.hugegraph.api.API;
import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang.StringUtils;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.config.CoreOptions;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.config.ServerOptions;
import com.baidu.hugegraph.rpc.RpcClientProviderWithAuth;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.StringEncoding;

public class StandardAuthenticator implements HugeAuthenticator {

    private static final String INITING_STORE = "initing_store";

    private HugeGraph graph = null;

    private HugeGraph graph() {
        E.checkState(this.graph != null, "Must setup Authenticator first");
        return this.graph;
    }

    private void initAdminUser() throws Exception {
        if (this.requireInitAdminUser()) {
            this.initAdminUser(this.inputPassword());
        }
        this.graph.close();
    }

    @Override
    public void initAdminUser(String password) {
        // Not allowed to call by non main thread
        String caller = Thread.currentThread().getName();
        E.checkState("main".equals(caller), "Invalid caller '%s'", caller);

        AuthManager authManager = this.graph().hugegraph().authManager();
        // Only init user when local mode and user has not been initialized
        if (this.requireInitAdminUser()) {
            HugeUser admin = new HugeUser(HugeAuthenticator.USER_ADMIN);
            admin.password(StringEncoding.hashPassword(password));
            admin.creator(HugeAuthenticator.USER_SYSTEM);
            authManager.createUser(admin);
        }
    }

    private boolean requireInitAdminUser() {
        AuthManager authManager = this.graph().hugegraph().authManager();
        return StandardAuthManager.isLocal(authManager) &&
               authManager.findUser(HugeAuthenticator.USER_ADMIN) == null;
    }

    private String inputPassword() {
        String inputPrompt = "Please input the admin password:";
        String notEmptyPrompt = "The admin password is 5-16 characters, " +
                                "which can be letters, numbers or " +
                                "special symbols";
        Console console = System.console();
        while (true) {
            String password = "";
            if (console != null) {
                char[] chars = console.readPassword(inputPrompt);
                password = new String(chars);
            } else {
                System.out.print(inputPrompt);
                @SuppressWarnings("resource") // just wrapper of System.in
                Scanner scanner = new Scanner(System.in);
                password = scanner.nextLine();
            }
            if (!password.isEmpty() &&
                password.matches(API.USER_PASSWORD_PATTERN)) {
                return password;
            }
            System.out.println(notEmptyPrompt);
        }
    }

    @Override
    public void setup(HugeConfig config) {
        String graphName = config.get(ServerOptions.AUTH_GRAPH_STORE);
        String graphPath = config.getMap(ServerOptions.GRAPHS).get(graphName);
        E.checkArgument(graphPath != null,
                        "Can't find graph name '%s' in config '%s' at " +
                        "'rest-server.properties' to store auth information, " +
                        "please ensure the value of '%s' matches it correctly",
                        graphName, ServerOptions.GRAPHS,
                        ServerOptions.AUTH_GRAPH_STORE.name());

        HugeConfig graphConfig = new HugeConfig(graphPath);
        if (config.getProperty(INITING_STORE) != null &&
            config.getBoolean(INITING_STORE)) {
            // Forced set RAFT_MODE to false when initializing backend
            graphConfig.setProperty(CoreOptions.RAFT_MODE.name(), "false");
        }
        this.graph = (HugeGraph) GraphFactory.open(graphConfig);

        String remoteUrl = config.get(ServerOptions.AUTH_REMOTE_URL);
        if (StringUtils.isNotEmpty(remoteUrl)) {
            RpcClientProviderWithAuth clientProvider =
                                      new RpcClientProviderWithAuth(config);
            this.graph.switchAuthManager(clientProvider.authManager());
        }
    }

    /**
     * Verify if a user is legal
     * @param username the username for authentication
     * @param password the password for authentication
     * @param token the token for authentication
     * @return String No permission if return ROLE_NONE else return a role
     */
    @Override
    public UserWithRole authenticate(String username, String password,
                                     String token) {
        UserWithRole userWithRole;
        if (StringUtils.isNotEmpty(token)) {
            userWithRole = this.authManager().validateUser(token);
        } else {
            E.checkArgumentNotNull(username,
                                   "The username parameter can't be null");
            E.checkArgumentNotNull(password,
                                   "The password parameter can't be null");
            userWithRole = this.authManager().validateUser(username, password);
        }

        RolePermission role = userWithRole.role();
        if (role == null) {
            role = ROLE_NONE;
        } else if (USER_ADMIN.equals(userWithRole.username())) {
            role = ROLE_ADMIN;
        } else {
            return userWithRole;
        }

        return new UserWithRole(userWithRole.userId(),
                                userWithRole.username(), role);
    }

    @Override
    public AuthManager authManager() {
        return this.graph().authManager();
    }

    @Override
    public SaslNegotiator newSaslNegotiator(InetAddress remoteAddress) {
        throw new NotImplementedException("SaslNegotiator is unsupported");
    }

    public static void initAdminUserIfNeeded(String confFile) throws Exception {
        StandardAuthenticator auth = new StandardAuthenticator();
        HugeConfig config = new HugeConfig(confFile);
        String authClass = config.get(ServerOptions.AUTHENTICATOR);
        if (authClass.isEmpty()) {
            return;
        }
        config.addProperty(INITING_STORE, true);
        auth.setup(config);
        if (auth.graph().backendStoreFeatures().supportsPersistence()) {
            auth.initAdminUser();
        }
    }
}
