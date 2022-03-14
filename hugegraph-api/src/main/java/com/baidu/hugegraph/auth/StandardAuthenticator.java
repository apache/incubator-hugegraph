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
import java.util.List;

import com.baidu.hugegraph.config.ServerOptions;
import com.baidu.hugegraph.meta.MetaManager;
import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang.StringUtils;

import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.util.E;

public class StandardAuthenticator implements HugeAuthenticator {

    private static final String INITING_STORE = "initing_store";

    private AuthManager authManager = null;

    @Override
    public void setup(HugeConfig config) {
        String cluster = config.get(ServerOptions.CLUSTER);
        List<String> endpoints = config.get(ServerOptions.META_ENDPOINTS);
        boolean useCa = config.get(ServerOptions.META_USE_CA);
        String ca = null;
        String clientCa = null;
        String clientKey = null;
        if (useCa) {
            ca = config.get(ServerOptions.META_CA);
            clientCa = config.get(ServerOptions.META_CLIENT_CA);
            clientKey = config.get(ServerOptions.META_CLIENT_KEY);
        }
        MetaManager metaManager = MetaManager.instance();
        metaManager.connect(cluster, MetaManager.MetaDriverType.ETCD,
                            ca, clientCa, clientKey, endpoints);
        this.authManager = new StandardAuthManager(metaManager,
                                                   config);
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
        E.checkState(this.authManager != null,
                     "Must setup authManager first");
        return this.authManager;
    }

    @Override
    public SaslNegotiator newSaslNegotiator(InetAddress remoteAddress) {
        throw new NotImplementedException("SaslNegotiator is unsupported");
    }

    public static void initAdminUserIfNeeded(String confFile,
                                             List<String> metaEndpoints,
                                             String cluster,
                                             Boolean withCa,
                                             String caFile,
                                             String clientCaFile,
                                             String clientKeyFile)
                                             throws Exception {
        MetaManager metaManager = MetaManager.instance();
        HugeConfig config = new HugeConfig(confFile);
        if (!withCa) {
            caFile = null;
            clientCaFile = null;
            clientKeyFile = null;
        }

        metaManager.connect(cluster, MetaManager.MetaDriverType.ETCD, caFile,
                            clientCaFile, clientKeyFile, metaEndpoints);
        StandardAuthManager authManager = new StandardAuthManager(metaManager,
                                                                  config);
        authManager.initAdmin();
    }

    public static void initAdminUserIfNeeded(HugeConfig config,
                                             List<String> metaEndpoints,
                                             String cluster,
                                             Boolean withCa,
                                             String caFile,
                                             String clientCaFile,
                                             String clientKeyFile) {
        MetaManager metaManager = MetaManager.instance();
        if (!withCa) {
            caFile = null;
            clientCaFile = null;
            clientKeyFile = null;
        }

        metaManager.connect(cluster, MetaManager.MetaDriverType.ETCD, caFile,
                            clientCaFile, clientKeyFile, metaEndpoints);
        StandardAuthManager authManager =
                            new StandardAuthManager(metaManager, config);
        authManager.initAdmin();
    }
}
