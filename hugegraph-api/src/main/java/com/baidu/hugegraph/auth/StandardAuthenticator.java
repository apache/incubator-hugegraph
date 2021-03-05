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

import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang.StringUtils;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.config.CoreOptions;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.config.ServerOptions;
import com.baidu.hugegraph.rpc.RpcClientProvider;
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
        // Not allowed to call by non main thread
        String caller = Thread.currentThread().getName();
        E.checkState(caller.equals("main"), "Invalid caller '%s'", caller);

        AuthManager authManager = this.graph().hugegraph().authManager();
        // Only init user when local mode and user has not been initialized
        if (StandardAuthManager.isLocal(authManager) &&
            authManager.findUser(HugeAuthenticator.USER_ADMIN) == null) {
            HugeUser admin = new HugeUser(HugeAuthenticator.USER_ADMIN);
            admin.password(StringEncoding.hashPassword(this.inputPassword()));
            admin.creator(HugeAuthenticator.USER_SYSTEM);
            authManager.createUser(admin);
        }

        this.graph.close();
    }

    private String inputPassword() {
        String prompt = "Please input the admin password:";
        Console console = System.console();
        if (console != null) {
            char[] chars = console.readPassword(prompt);
            return new String(chars);
        } else {
            System.out.print(prompt);
            @SuppressWarnings("resource") // just wrapper of System.in
            Scanner scanner = new Scanner(System.in);
            return scanner.nextLine();
        }
    }

    @Override
    public void setup(HugeConfig config) {
        String graphName = config.get(ServerOptions.AUTH_GRAPH_STORE);
        String graphPath = config.getMap(ServerOptions.GRAPHS).get(graphName);
        E.checkArgument(graphPath != null,
                        "Invalid graph name '%s'", graphName);
        HugeConfig graphConfig = new HugeConfig(graphPath);
        if (config.getProperty(INITING_STORE) != null &&
            config.getBoolean(INITING_STORE)) {
            // Forced set RAFT_MODE to false when initializing backend
            graphConfig.setProperty(CoreOptions.RAFT_MODE.name(), "false");
        }
        this.graph = (HugeGraph) GraphFactory.open(graphConfig);

        String remoteUrl = config.get(ServerOptions.AUTH_REMOTE_URL);
        if (StringUtils.isNotEmpty(remoteUrl)) {
            RpcClientProvider provider = new RpcClientProvider(config);
            this.graph.switchAuthManager(provider.authManager());
        }
    }

    /**
     * Verify if a user is legal
     * @param username  the username for authentication
     * @param password  the password for authentication
     * @return String No permission if return ROLE_NONE else return a role
     */
    @Override
    public RolePermission authenticate(String username, String password) {
        E.checkArgumentNotNull(username,
                               "The username parameter can't be null");
        E.checkArgumentNotNull(password,
                               "The password parameter can't be null");

        RolePermission role = this.graph().authManager().loginUser(username,
                                                                   password);
        if (role == null) {
            role = ROLE_NONE;
        } else if (username.equals(USER_ADMIN)) {
            role = ROLE_ADMIN;
        }
        return role;
    }

    @Override
    public AuthManager authManager() {
        return this.graph().authManager();
    }

    @Override
    public SaslNegotiator newSaslNegotiator(InetAddress remoteAddress) {
        throw new NotImplementedException("SaslNegotiator is unsupported");
    }

    public static void initAdminUser(String restConfFile) throws Exception {
        StandardAuthenticator auth = new StandardAuthenticator();
        HugeConfig config = new HugeConfig(restConfFile);
        String authClass = config.get(ServerOptions.AUTHENTICATOR);
        if (authClass.isEmpty()) {
            return;
        }
        config.addProperty(INITING_STORE, true);
        auth.setup(config);
        auth.initAdminUser();
    }
}
