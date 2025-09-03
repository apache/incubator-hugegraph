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

package org.apache.hugegraph.pd.client;

import java.util.Base64;

import static java.nio.charset.StandardCharsets.UTF_8;

public final class PDConfig {

    private static final int GRPC_DEFAULT_MAX_INBOUND_MESSAGE_SIZE = 1024 * 1024 * 1024;
    private static final int GRPC_DEFAULT_MAX_OUTBOUND_MESSAGE_SIZE = 1024 * 1024 * 1024;
    private static final int inboundMessageSize = GRPC_DEFAULT_MAX_INBOUND_MESSAGE_SIZE;
    private static final int outboundMessageSize = GRPC_DEFAULT_MAX_OUTBOUND_MESSAGE_SIZE;
    // Whether to receive asynchronous PD notifications
    private final boolean enablePDNotify = false;
    private boolean enableCache = false;
    // TODO: multi-server
    private String serverHost = "localhost:9000";
    // The timeout period for grpc call is 10 seconds
    private long grpcTimeOut = 60000;
    private String authority;
    private String userName = "";

    private PDConfig() {
    }

    public String getServerHost() {
        return serverHost;
    }

    public long getGrpcTimeOut() {
        return grpcTimeOut;
    }

    public static PDConfig of() {
        return new PDConfig();
    }

    public static PDConfig of(String serverHost) {
        PDConfig config = new PDConfig();
        config.serverHost = serverHost;
        return config;
    }

    public PDConfig setEnableCache(boolean enableCache) {
        this.enableCache = enableCache;
        return this;
    }


    public static PDConfig of(String serverHost, long timeOut) {
        PDConfig config = new PDConfig();
        config.serverHost = serverHost;
        config.grpcTimeOut = timeOut;
        return config;
    }

    @Deprecated
    public PDConfig setEnablePDNotify(boolean enablePDNotify) {
        return this;
    }

    public boolean isEnableCache() {
        return enableCache;
    }

    @Override
    public String toString() {
        return "PDConfig{ serverHost='" + serverHost + '\'' + '}';
    }

    public PDConfig setAuthority(String userName, String pwd) {
        this.userName = userName;
        String auth = userName + ':' + pwd;
        this.authority = new String(Base64.getEncoder().encode(auth.getBytes(UTF_8)));
        return this;
    }

}
