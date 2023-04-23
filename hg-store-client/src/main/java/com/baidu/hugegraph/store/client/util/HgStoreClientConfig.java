package com.baidu.hugegraph.store.client.util;

import java.util.PropertyResourceBundle;

import lombok.extern.slf4j.Slf4j;

/**
 * @author lynn.bond@hotmail.com on 2021/11/29
 */
@Slf4j
public final class HgStoreClientConfig {

    private static final int GRPC_DEFAULT_TIMEOUT_SECONDS = 100;
    private static final int GRPC_DEFAULT_MAX_INBOUND_MESSAGE_SIZE = 1024 * 1024 * 1024;
    private static final int GRPC_DEFAULT_MAX_OUTBOUND_MESSAGE_SIZE = 1024 * 1024 * 1024;

    private static final int NET_KV_SCANNER_PAGE_SIZE = 10_000;
    private static final int NET_KV_SCANNER_HAVE_NEXT_TIMEOUT = 30 * 60;

    private Integer grpcTimeoutSeconds = GRPC_DEFAULT_TIMEOUT_SECONDS;
    private Integer grpcMaxInboundMessageSize = GRPC_DEFAULT_MAX_INBOUND_MESSAGE_SIZE;
    private Integer grpcMaxOutboundMessageSize = GRPC_DEFAULT_MAX_OUTBOUND_MESSAGE_SIZE;

    private Integer netKvScannerPageSize = NET_KV_SCANNER_PAGE_SIZE;
    private Integer netKvScannerHaveNextTimeout = NET_KV_SCANNER_HAVE_NEXT_TIMEOUT;

    private static PropertyResourceBundle prb = null;
    private static String fileName = "hg-store-client";

    private static HgStoreClientConfig defaultInstance;


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

    private HgStoreClientConfig() {
    }

    public Integer getGrpcTimeoutSeconds() {
        return grpcTimeoutSeconds;
    }

    public Integer getGrpcMaxInboundMessageSize() {
        return grpcMaxInboundMessageSize;
    }

    public Integer getGrpcMaxOutboundMessageSize() {
        return grpcMaxOutboundMessageSize;
    }

    public Integer getNetKvScannerPageSize() {
        return netKvScannerPageSize;
    }

    public Integer getNetKvScannerHaveNextTimeout() {
        return netKvScannerHaveNextTimeout;
    }

    public HgStoreClientConfig setGrpcTimeoutSeconds(Integer grpcTimeoutSeconds) {
        this.grpcTimeoutSeconds = grpcTimeoutSeconds;
        return this;
    }

    public HgStoreClientConfig setGrpcMaxInboundMessageSize(Integer grpcMaxInboundMessageSize) {
        this.grpcMaxInboundMessageSize = grpcMaxInboundMessageSize;
        return this;
    }

    public HgStoreClientConfig setGrpcMaxOutboundMessageSize(Integer grpcMaxOutboundMessageSize) {
        this.grpcMaxOutboundMessageSize = grpcMaxOutboundMessageSize;
        return this;
    }

    public HgStoreClientConfig setNetKvScannerPageSize(Integer netKvScannerPageSize) {
        this.netKvScannerPageSize = netKvScannerPageSize;
        return this;
    }

    public HgStoreClientConfig setNetKvScannerHaveNextTimeout(Integer netKvScannerHaveNextTimeout) {
        this.netKvScannerHaveNextTimeout = netKvScannerHaveNextTimeout;
        return this;
    }

    private static class PropertiesWrapper {
        private PropertyResourceBundle prb;

        PropertiesWrapper(PropertyResourceBundle prb) {
            this.prb = prb;
        }

        Integer getInt(String key, Integer defaultValue) {

            String buf = this.getStr(key);
            if (buf == null || buf.isEmpty()) return defaultValue;

            Integer res = null;
            try {
                res = Integer.valueOf(buf);
            } catch (Throwable t) {
                log.error("Failed to parse a int value[ " + buf + " ] of the key[ " + key + " ].", t);
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
