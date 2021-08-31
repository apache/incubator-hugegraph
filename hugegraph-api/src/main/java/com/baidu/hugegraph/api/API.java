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

package com.baidu.hugegraph.api;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.function.Consumer;

import javax.ws.rs.ForbiddenException;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.NotSupportedException;
import javax.ws.rs.core.MediaType;

import org.slf4j.Logger;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.core.GraphManager;
import com.baidu.hugegraph.define.Checkable;
import com.baidu.hugegraph.metrics.MetricsUtil;
import com.baidu.hugegraph.server.RestServer;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.JsonUtil;
import com.baidu.hugegraph.util.Log;
import com.codahale.metrics.Meter;
import com.google.common.collect.ImmutableMap;

public class API {

    private static final Logger LOG = Log.logger(RestServer.class);

    public static final String CHARSET = "UTF-8";

    public static final String APPLICATION_JSON = MediaType.APPLICATION_JSON;
    public static final String APPLICATION_JSON_WITH_CHARSET =
                               APPLICATION_JSON + ";charset=" + CHARSET;
    public static final String JSON = MediaType.APPLICATION_JSON_TYPE
                                               .getSubtype();

    public static final String ACTION_APPEND = "append";
    public static final String ACTION_ELIMINATE = "eliminate";
    public static final String ACTION_CLEAR = "clear";

    public static final String USER_NAME_PATTERN = "^[0-9a-zA-Z_]{5,16}$";
    public static final String USER_PASSWORD_PATTERN =
           "[a-zA-Z0-9~!@#$%^&*()_+|<>,.?/:;'`\"\\[\\]{}\\\\]{5,16}";

    private static final Meter succeedMeter =
                         MetricsUtil.registerMeter(API.class, "commit-succeed");
    private static final Meter illegalArgErrorMeter =
                         MetricsUtil.registerMeter(API.class, "illegal-arg");
    private static final Meter expectedErrorMeter =
                         MetricsUtil.registerMeter(API.class, "expected-error");
    private static final Meter unknownErrorMeter =
                         MetricsUtil.registerMeter(API.class, "unknown-error");

    public static final String SYSTEM_GRAPH = "system";

    public static HugeGraph graph(GraphManager manager, String graph) {
        HugeGraph g = manager.graph(graph);
        if (g == null) {
            throw new NotFoundException(String.format(
                      "Graph '%s' does not exist",  graph));
        }
        return g;
    }

    public static HugeGraph graph4admin(GraphManager manager, String graph) {
        return graph(manager, graph).hugegraph();
    }

    public static <R> R commit(HugeGraph g, Callable<R> callable) {
        Consumer<Throwable> rollback = (error) -> {
            if (error != null) {
                LOG.error("Failed to commit", error);
            }
            try {
                g.tx().rollback();
            } catch (Throwable e) {
                LOG.error("Failed to rollback", e);
            }
        };

        try {
            R result = callable.call();
            g.tx().commit();
            succeedMeter.mark();
            return result;
        } catch (IllegalArgumentException | NotFoundException |
                 ForbiddenException e) {
            illegalArgErrorMeter.mark();
            rollback.accept(null);
            throw e;
        } catch (RuntimeException e) {
            expectedErrorMeter.mark();
            rollback.accept(e);
            throw e;
        } catch (Throwable e) {
            unknownErrorMeter.mark();
            rollback.accept(e);
            // TODO: throw the origin exception 'e'
            throw new HugeException("Failed to commit", e);
        }
    }

    public static void commit(HugeGraph g, Runnable runnable) {
        commit(g, () -> {
            runnable.run();
            return null;
        });
    }

    public static Object[] properties(Map<String, Object> properties) {
        Object[] list = new Object[properties.size() * 2];
        int i = 0;
        for (Map.Entry<String, Object> prop : properties.entrySet()) {
            list[i++] = prop.getKey();
            list[i++] = prop.getValue();
        }
        return list;
    }

    protected static void checkCreatingBody(Checkable body) {
        E.checkArgumentNotNull(body, "The request body can't be empty");
        body.checkCreate(false);
    }

    protected static void checkUpdatingBody(Checkable body) {
        E.checkArgumentNotNull(body, "The request body can't be empty");
        body.checkUpdate();
    }

    protected static void checkCreatingBody(
                          Collection<? extends Checkable> bodys) {
        E.checkArgumentNotNull(bodys, "The request body can't be empty");
        for (Checkable body : bodys) {
            E.checkArgument(body != null,
                            "The batch body can't contain null record");
            body.checkCreate(true);
        }
    }

    protected static void checkUpdatingBody(
                          Collection<? extends Checkable> bodys) {
        E.checkArgumentNotNull(bodys, "The request body can't be empty");
        for (Checkable body : bodys) {
            E.checkArgumentNotNull(body,
                                   "The batch body can't contain null record");
            body.checkUpdate();
        }
    }

    @SuppressWarnings("unchecked")
    protected static Map<String, Object> parseProperties(String properties) {
        if (properties == null || properties.isEmpty()) {
            return ImmutableMap.of();
        }

        Map<String, Object> props = null;
        try {
            props = JsonUtil.fromJson(properties, Map.class);
        } catch (Exception ignored) {}

        // If properties is the string "null", props will be null
        E.checkArgument(props != null,
                        "Invalid request with properties: %s", properties);
        return props;
    }

    public static boolean checkAndParseAction(String action) {
        E.checkArgumentNotNull(action, "The action param can't be empty");
        if (action.equals(ACTION_APPEND)) {
            return true;
        } else if (action.equals(ACTION_ELIMINATE)) {
            return false;
        } else {
            throw new NotSupportedException(
                      String.format("Not support action '%s'", action));
        }
    }
}
