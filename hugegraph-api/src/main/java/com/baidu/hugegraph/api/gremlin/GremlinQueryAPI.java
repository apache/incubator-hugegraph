package com.baidu.hugegraph.api.gremlin;

import java.util.Map;
import java.util.Set;

import com.baidu.hugegraph.api.API;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.config.ServerOptions;
import com.baidu.hugegraph.exception.HugeGremlinException;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import jakarta.inject.Provider;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

public class GremlinQueryAPI extends API {

    private static final Set<String> FORBIDDEN_REQUEST_EXCEPTIONS =
            ImmutableSet.of("java.lang.SecurityException",
                            "jakarta.ws.rs.ForbiddenException");
    private static final Set<String> BAD_REQUEST_EXCEPTIONS = ImmutableSet.of(
            "java.lang.IllegalArgumentException",
            "java.util.concurrent.TimeoutException",
            "groovy.lang.",
            "org.codehaus.",
            "com.baidu.hugegraph."
    );

    @Context
    private Provider<HugeConfig> configProvider;

    private GremlinClient client;

    public GremlinClient client() {
        if (this.client != null) {
            return this.client;
        }
        HugeConfig config = this.configProvider.get();
        String url = config.get(ServerOptions.GREMLIN_SERVER_URL);
        int timeout = config.get(ServerOptions.GREMLIN_SERVER_TIMEOUT) * 1000;
        int maxRoutes = config.get(ServerOptions.GREMLIN_SERVER_MAX_ROUTE);
        this.client = new GremlinClient(url, timeout, maxRoutes, maxRoutes);
        return this.client;
    }

    protected static Response transformResponseIfNeeded(Response response) {
        MediaType mediaType = response.getMediaType();
        if (mediaType != null) {
            // Append charset
            assert MediaType.APPLICATION_JSON_TYPE.equals(mediaType);
            response.getHeaders().putSingle(HttpHeaders.CONTENT_TYPE,
                                            mediaType.withCharset(CHARSET));
        }

        Response.StatusType status = response.getStatusInfo();
        if (status.getStatusCode() < 400) {
            // No need to transform if normal response without error
            return response;
        }

        if (mediaType == null || !JSON.equals(mediaType.getSubtype())) {
            String message = response.readEntity(String.class);
            throw new HugeGremlinException(status.getStatusCode(),
                                           ImmutableMap.of("message", message));
        }

        @SuppressWarnings("unchecked")
        Map<String, Object> map = response.readEntity(Map.class);
        String exClassName = (String) map.get("Exception-Class");
        if (FORBIDDEN_REQUEST_EXCEPTIONS.contains(exClassName)) {
            status = Response.Status.FORBIDDEN;
        } else if (matchBadRequestException(exClassName)) {
            status = Response.Status.BAD_REQUEST;
        }
        throw new HugeGremlinException(status.getStatusCode(), map);
    }

    private static boolean matchBadRequestException(String exClass) {
        if (exClass == null) {
            return false;
        }
        if (BAD_REQUEST_EXCEPTIONS.contains(exClass)) {
            return true;
        }
        return BAD_REQUEST_EXCEPTIONS.stream().anyMatch(exClass::startsWith);
    }
}
