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

package org.apache.hugegraph.api.filter;

import java.io.IOException;

import jakarta.inject.Singleton;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.container.PreMatching;
import jakarta.ws.rs.ext.Provider;

@Provider
@Singleton
@PreMatching
public class PathFilter implements ContainerRequestFilter {

    public static final String REQUEST_TIME = "request_time";
    public static final String REQUEST_PARAMS_JSON = "request_params_json";

    @Override
    public void filter(ContainerRequestContext context) throws IOException {
        context.setProperty(REQUEST_TIME, System.currentTimeMillis());

        // TODO: temporarily comment it to fix loader bug, handle it later
        /*// record the request json
        String method = context.getMethod();
        String requestParamsJson = "";
        if (method.equals(HttpMethod.POST)) {
            requestParamsJson = IOUtils.toString(context.getEntityStream(),
                                                 Charsets.toCharset(CHARSET));
            // replace input stream because we have already read it
            InputStream in = IOUtils.toInputStream(requestParamsJson, Charsets.toCharset(CHARSET));
            context.setEntityStream(in);
        } else if (method.equals(HttpMethod.GET)) {
            MultivaluedMap<String, String> pathParameters = context.getUriInfo()
                                                                   .getPathParameters();
            requestParamsJson = pathParameters.toString();
        }

        context.setProperty(REQUEST_PARAMS_JSON, requestParamsJson);*/
    }
}
