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

package org.apache.hugegraph.api.gremlin;

import java.util.List;
import java.util.Map;

import org.apache.hugegraph.api.filter.CompressInterceptor;
import org.apache.hugegraph.util.E;

import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.client.WebTarget;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.MultivaluedMap;
import jakarta.ws.rs.core.Response;

/**
 * GremlinClient is a client for interacting with a Gremlin server.
 * It extends the AbstractJerseyRestClient and provides methods for sending GET and POST requests.
 */
public class GremlinClient extends AbstractJerseyRestClient {

    /**
     * Constructs a GremlinClient with the specified URL, timeout, maxTotal, and maxPerRoute.
     *
     * @param url         The URL of the Gremlin server this client will interact with.
     * @param timeout     The timeout for the client.
     * @param maxTotal    The maximum total connections for the client.
     * @param maxPerRoute The maximum connections per route for the client.
     */
    public GremlinClient(String url, int timeout, int maxTotal, int maxPerRoute) {
        super(url, timeout, maxTotal, maxPerRoute);
    }

    /**
     * Sends a POST request to the Gremlin server.
     *
     * @param auth The authorization token for the request.
     * @param req  The body of the request.
     * @return The response from the server.
     */
    public Response doPostRequest(String auth, String req) {
        Entity<?> body = Entity.entity(req, MediaType.APPLICATION_JSON);
        return this.getWebTarget().request()
                   .header(HttpHeaders.AUTHORIZATION, auth)
                   .accept(MediaType.APPLICATION_JSON)
                   .acceptEncoding(CompressInterceptor.GZIP)
                   .post(body);
    }

    /**
     * Sends a GET request to the Gremlin server.
     *
     * @param auth   The authorization token for the request.
     * @param params The query parameters for the request.
     * @return The response from the server.
     */
    public Response doGetRequest(String auth, MultivaluedMap<String, String> params) {
        WebTarget target = this.getWebTarget();
        for (Map.Entry<String, List<String>> entry : params.entrySet()) {
            E.checkArgument(entry.getValue().size() == 1,
                            "Invalid query param '%s', can only accept one value, but got %s",
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
