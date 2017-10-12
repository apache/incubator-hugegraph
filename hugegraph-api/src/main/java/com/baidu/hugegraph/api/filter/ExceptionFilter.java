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

package com.baidu.hugegraph.api.filter;

import javax.json.Json;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObjectBuilder;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.exception.NotFoundException;

public class ExceptionFilter {

    public static final boolean ALLOW_TRACE = false;

    @Provider
    public static class HugeExceptionMapper
                  implements ExceptionMapper<HugeException> {

        @Override
        public Response toResponse(HugeException exception) {
            return Response.status(400)
                           .type(MediaType.APPLICATION_JSON)
                           .entity(formatException(exception))
                           .build();
        }
    }

    @Provider
    public static class IllegalArgumentExceptionMapper
                  implements ExceptionMapper<IllegalArgumentException> {

        @Override
        public Response toResponse(IllegalArgumentException exception) {
            return Response.status(400)
                           .type(MediaType.APPLICATION_JSON)
                           .entity(formatException(exception))
                           .build();
        }
    }

    @Provider
    public static class NotFoundExceptionMapper
                  implements ExceptionMapper<NotFoundException> {

        @Override
        public Response toResponse(NotFoundException exception) {
            return Response.status(404)
                           .type(MediaType.APPLICATION_JSON)
                           .entity(formatException(exception))
                           .build();
        }
    }

    @Provider
    public static class WebApplicationExceptionMapper
                  implements ExceptionMapper<WebApplicationException> {

        private static final int INTERNAL_SERVER_ERROR =
                Response.Status.INTERNAL_SERVER_ERROR.getStatusCode();

        @Override
        public Response toResponse(WebApplicationException exception) {

            Response response = exception.getResponse();
            if (response.hasEntity()) {
                return response;
            }

            boolean trace = response.getStatus() == INTERNAL_SERVER_ERROR;
            return Response.status(response.getStatus())
                           .type(MediaType.APPLICATION_JSON)
                           .entity(formatException(exception, trace))
                           .build();
        }
    }

    @Provider
    public static class UnknownExceptionMapper
                  implements ExceptionMapper<Exception> {

        @Override
        public Response toResponse(Exception exception) {
            return Response.status(500)
                           .type(MediaType.APPLICATION_JSON)
                           .entity(formatException(exception, true))
                           .build();
        }
    }

    public static String formatException(Exception exception) {
        return formatException(exception, false);
    }

    public static String formatException(Exception exception, boolean trace) {
        String msg = exception.getMessage() != null ?
                     exception.getMessage() : "";
        String cause = exception.getCause() != null ?
                       exception.getCause().toString() : "";

        JsonObjectBuilder json = Json.createObjectBuilder()
                .add("exception", exception.getClass().toString())
                .add("message", msg)
                .add("cause", cause);

        if (ALLOW_TRACE && trace) {
            JsonArrayBuilder traces = Json.createArrayBuilder();
            for (StackTraceElement i : exception.getStackTrace()) {
                traces.add(i.toString());
            }
            json.add("trace", traces);
        }

        return json.build().toString();
    }
}
