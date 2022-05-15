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

package com.baidu.hugegraph.api.gremlin;

import java.util.List;
import java.util.Map;

import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.client.WebTarget;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.MultivaluedMap;
import jakarta.ws.rs.core.Response;

import com.baidu.hugegraph.api.filter.CompressInterceptor;
import com.baidu.hugegraph.rest.AbstractRestClient;
import com.baidu.hugegraph.testutil.Whitebox;
import com.baidu.hugegraph.util.E;

public class GremlinClient extends AbstractRestClient {

    private final WebTarget webTarget;

    public GremlinClient(String url, int timeout,
                         int maxTotal, int maxPerRoute) {
        super(url, timeout, maxTotal, maxPerRoute);
        this.webTarget = Whitebox.getInternalState(this, "target");
        E.checkNotNull(this.webTarget, "target");
    }

    @Override
    protected void checkStatus(Response response, Response.Status... statuses) {
        // pass
    }

    public Response doPostRequest(String auth, String req) {
        Entity<?> body = Entity.entity(req, MediaType.APPLICATION_JSON);
        return this.webTarget.request()
                             .header(HttpHeaders.AUTHORIZATION, auth)
                             .accept(MediaType.APPLICATION_JSON)
                             .acceptEncoding(CompressInterceptor.GZIP)
                             .post(body);
    }

    public Response doGetRequest(String auth,
                                 MultivaluedMap<String, String> params) {
        WebTarget target = this.webTarget;
        for (Map.Entry<String, List<String>> entry : params.entrySet()) {
            E.checkArgument(entry.getValue().size() == 1,
                            "Invalid query param '%s', can only accept " +
                            "one value, but got %s",
                            entry.getKey(), entry.getValue());
            target = target.queryParam(entry.getKey(), entry.getValue().get(0));
        }
        return target.request()
                     .header(HttpHeaders.AUTHORIZATION, auth)
                     .accept(MediaType.APPLICATION_JSON)
                     .acceptEncoding(CompressInterceptor.GZIP)
                     .get();
    }
}
