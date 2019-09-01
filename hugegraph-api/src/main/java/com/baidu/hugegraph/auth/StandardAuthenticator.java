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

import org.apache.commons.lang.NotImplementedException;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;

import com.baidu.hugegraph.GremlinGraph;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.config.ServerOptions;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.StringEncoding;

public class StandardAuthenticator implements HugeAuthenticator {

    private GremlinGraph graph = null;

    @Override
    public void setup(HugeConfig config) {
        String graphName = config.get(ServerOptions.AUTH_GRAPH_STORE);
        String graphPath = config.getMap(ServerOptions.GRAPHS).get(graphName);
        E.checkArgument(graphPath != null,
                        "Invalid graph name '%s'", graphName);
        this.graph = (GremlinGraph) GraphFactory.open(graphPath);
    }

    protected String matchUser(String username, String password) {
        E.checkState(this.graph != null, "Must setup Authenticator first");
        return this.graph.matchUser(username, password);
    }

    /**
     * Verify if a user is legal
     * @param username  the username for authentication
     * @param password  the password for authentication
     * @return String No permission if return ROLE_NONE else return a role
     */
    @Override
    public String authenticate(final String username, final String password) {
        E.checkArgumentNotNull(username,
                               "The username parameter can't be null");
        E.checkArgumentNotNull(password,
                               "The password parameter can't be null");

        String role = this.matchUser(username, StringEncoding.sha256(password));
        if (role == null) {
            role = ROLE_NONE;
        } else if (username.equals(User.USER_ADMIN)) {
            role = ROLE_ADMIN;
        }
        return role;
    }

    @Override
    public SaslNegotiator newSaslNegotiator(InetAddress remoteAddress) {
        throw new NotImplementedException("SaslNegotiator is unsupported");
    }
}
