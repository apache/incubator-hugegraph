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

import static org.apache.hugegraph.api.API.CHARSET;

import java.io.BufferedInputStream;
import java.io.IOException;

import org.apache.commons.io.Charsets;
import org.apache.commons.io.IOUtils;

import jakarta.inject.Singleton;
import jakarta.ws.rs.HttpMethod;
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
    public static final int MAX_SLOW_LOG_BODY_LENGTH = 512;

    @Override
    public void filter(ContainerRequestContext context) throws IOException {
        long startTime = System.currentTimeMillis();

        context.setProperty(REQUEST_TIME, startTime);

        collectRequestParams(context);
    }

    private void collectRequestParams(ContainerRequestContext context) throws IOException {
        String method = context.getMethod();
        if (method.equals(HttpMethod.POST) || method.equals(HttpMethod.PUT) ||
            method.equals(HttpMethod.DELETE)) {
            BufferedInputStream bufferedStream = new BufferedInputStream(context.getEntityStream());

            bufferedStream.mark(Integer.MAX_VALUE);
            String body = IOUtils.toString(bufferedStream,
                                           Charsets.toCharset(CHARSET));
            body = body.length() > MAX_SLOW_LOG_BODY_LENGTH ?
                   body.substring(0, MAX_SLOW_LOG_BODY_LENGTH) : body;

            context.setProperty(REQUEST_PARAMS_JSON, body);

            bufferedStream.reset();

            context.setEntityStream(bufferedStream);
        } else if (method.equals(HttpMethod.GET)) {
            context.setProperty(REQUEST_PARAMS_JSON,
                                context.getUriInfo().getPathParameters().toString());
        }
    }
}
