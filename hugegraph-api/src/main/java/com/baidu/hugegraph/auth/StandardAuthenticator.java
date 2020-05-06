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
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.config.ServerOptions;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.StringEncoding;

public class StandardAuthenticator implements HugeAuthenticator {

    private HugeGraph graph = null;

    private HugeGraph graph() {
        E.checkState(this.graph != null, "Must setup Authenticator first");
        return this.graph;
    }

    private void initAdminUser() throws Exception {
        // Not allowed to call by non main thread
        String caller = Thread.currentThread().getName();
        E.checkState(caller.equals("main"), "Invalid caller '%s'", caller);

        UserManager userManager = this.graph().hugegraph().userManager();
        HugeUser admin = new HugeUser(HugeAuthenticator.USER_ADMIN);
        admin.password(StringEncoding.hashPassword(inputPassword()));
        admin.creator(HugeAuthenticator.USER_SYSTEM);
        userManager.createUser(admin);

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
            @SuppressWarnings("resource")
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
        this.graph = (HugeGraph) GraphFactory.open(graphPath);
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

        RolePermission role = this.graph().userManager().loginUser(username,
                                                                   password);
        if (role == null) {
            role = ROLE_NONE;
        } else if (username.equals(USER_ADMIN)) {
            role = ROLE_ADMIN;
        }
        return role;
    }

    @Override
    public UserManager userManager() {
        return this.graph().userManager();
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
        auth.setup(config);
        auth.initAdminUser();
    }
}
