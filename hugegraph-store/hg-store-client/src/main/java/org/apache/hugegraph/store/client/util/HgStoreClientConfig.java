/*
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

package org.apache.hugegraph.store.client.util;

import java.util.PropertyResourceBundle;

import lombok.extern.slf4j.Slf4j;

/**
 * 2021/11/29
 */
@Slf4j
public final class HgStoreClientConfig {

    private static final int GRPC_DEFAULT_TIMEOUT_SECONDS = 100;
    private static final int GRPC_DEFAULT_MAX_INBOUND_MESSAGE_SIZE = 1024 * 1024 * 1024;
    private static final int GRPC_DEFAULT_MAX_OUTBOUND_MESSAGE_SIZE = 1024 * 1024 * 1024;

    private static final int NET_KV_SCANNER_PAGE_SIZE = 10_000;
    private static final int NET_KV_SCANNER_HAVE_NEXT_TIMEOUT = 30 * 60;
    private static final String fileName = "hg-store-client";
    private static PropertyResourceBundle prb = null;
    private static HgStoreClientConfig defaultInstance;
    private Integer grpcTimeoutSeconds = GRPC_DEFAULT_TIMEOUT_SECONDS;
    private Integer grpcMaxInboundMessageSize = GRPC_DEFAULT_MAX_INBOUND_MESSAGE_SIZE;
    private Integer grpcMaxOutboundMessageSize = GRPC_DEFAULT_MAX_OUTBOUND_MESSAGE_SIZE;
    private Integer netKvScannerPageSize = NET_KV_SCANNER_PAGE_SIZE;
    private Integer netKvScannerHaveNextTimeout = NET_KV_SCANNER_HAVE_NEXT_TIMEOUT;


    private HgStoreClientConfig() {
    }

    public synchronized static HgStoreClientConfig of() {

        if (defaultInstance != null) {
            return defaultInstance;
        }

        defaultInstance = new HgStoreClientConfig();

        overrideViaProperties(defaultInstance);

        return defaultInstance;
    }

    private static void overrideViaProperties(HgStoreClientConfig config) {
        try {
            prb = (PropertyResourceBundle) PropertyResourceBundle.getBundle(fileName);
        } catch (Throwable t) {
            log.warn("Failed to load " + fileName + ".properties.");
            log.info("Default configuration was activated.");
            return;
        }
        PropertiesWrapper wrapper = new PropertiesWrapper(prb);

        log.info("grpc.timeout.seconds = "
                 + (config.grpcTimeoutSeconds = wrapper.getInt("grpc.timeout.seconds"
                , config.grpcTimeoutSeconds))
        );
        log.info("grpc.max.inbound.message.size = "
                 + (config.grpcMaxInboundMessageSize = GRPC_DEFAULT_MAX_INBOUND_MESSAGE_SIZE)
        );
        log.info("grpc.max.outbound.message.size = "
                 + (config.grpcMaxOutboundMessageSize = GRPC_DEFAULT_MAX_OUTBOUND_MESSAGE_SIZE)
        );
        log.info("net.kv.scanner.page.size = "
                 + (config.netKvScannerPageSize = wrapper.getInt("net.kv.scanner.page.size"
                , config.netKvScannerPageSize))
        );
        log.info("net.kv.scanner.have.next.timeout = {}", config.netKvScannerHaveNextTimeout);
    }

    public Integer getGrpcTimeoutSeconds() {
        return grpcTimeoutSeconds;
    }

    public HgStoreClientConfig setGrpcTimeoutSeconds(Integer grpcTimeoutSeconds) {
        this.grpcTimeoutSeconds = grpcTimeoutSeconds;
        return this;
    }

    public Integer getGrpcMaxInboundMessageSize() {
        return grpcMaxInboundMessageSize;
    }

    public HgStoreClientConfig setGrpcMaxInboundMessageSize(Integer grpcMaxInboundMessageSize) {
        this.grpcMaxInboundMessageSize = grpcMaxInboundMessageSize;
        return this;
    }

    public Integer getGrpcMaxOutboundMessageSize() {
        return grpcMaxOutboundMessageSize;
    }

    public HgStoreClientConfig setGrpcMaxOutboundMessageSize(Integer grpcMaxOutboundMessageSize) {
        this.grpcMaxOutboundMessageSize = grpcMaxOutboundMessageSize;
        return this;
    }

    public Integer getNetKvScannerPageSize() {
        return netKvScannerPageSize;
    }

    public HgStoreClientConfig setNetKvScannerPageSize(Integer netKvScannerPageSize) {
        this.netKvScannerPageSize = netKvScannerPageSize;
        return this;
    }

    public Integer getNetKvScannerHaveNextTimeout() {
        return netKvScannerHaveNextTimeout;
    }

    public HgStoreClientConfig setNetKvScannerHaveNextTimeout(Integer netKvScannerHaveNextTimeout) {
        this.netKvScannerHaveNextTimeout = netKvScannerHaveNextTimeout;
        return this;
    }

    private static class PropertiesWrapper {
        private final PropertyResourceBundle prb;

        PropertiesWrapper(PropertyResourceBundle prb) {
            this.prb = prb;
        }

        Integer getInt(String key, Integer defaultValue) {

            String buf = this.getStr(key);
            if (buf == null || buf.isEmpty()) {
                return defaultValue;
            }

            Integer res = null;
            try {
                res = Integer.valueOf(buf);
            } catch (Throwable t) {
                log.error("Failed to parse a int value[ " + buf + " ] of the key[ " + key + " ].",
                          t);
            }

            return res;

        }

        String getStr(String key, String defaultValue) {
            String res = getStr(key);

            if (res == null && defaultValue != null) {
                return defaultValue;
            }

            return res;
        }

        String getStr(String key) {
            String value = null;

            if (!prb.containsKey(key)) {
                return null;
            }

            try {
                value = prb.getString(key);
            } catch (Exception e) {
                log.warn("Failed to get value with key: [" + key + "]");
                return null;
            }

            if (value != null) {
                value = value.trim();
            }

            return value;
        }
    }
}
