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

import com.baidu.hugegraph.auth.HugePermission;
import com.baidu.hugegraph.util.DateUtil;
import com.baidu.hugegraph.util.Log;
import org.slf4j.Logger;

import javax.inject.Singleton;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import javax.ws.rs.core.SecurityContext;
import javax.ws.rs.ext.Provider;
import java.io.IOException;
import java.util.Date;
import java.util.Optional;


/**
 * AccessLogFilter performs as a middleware that log all the http accesses.
 * @since 2021-11-24
 */
@Provider
@Singleton
public class AccessLogFilter implements ContainerResponseFilter {

    private static final Logger LOG = Log.logger(AccessLogFilter.class);

    /**
     * Use filter to log request info
     * @param requestContext requestContext
     * @param responseContext responseContext
     */
    @Override
    public void filter(
            ContainerRequestContext requestContext,
            ContainerResponseContext responseContext) throws IOException {
        // Grab corresponding request / response info from context;
        String method = requestContext.getRequest().getMethod();
        String path = requestContext.getUriInfo().getPath();
        int code = responseContext.getStatus();

        String userName = "anonymous";
        String userId = "";
        String roles = HugePermission.NONE.string();

        // Calculate response time
        Date accessTime = Optional.ofNullable(requestContext.getDate()).orElse(DateUtil.DATE_ZERO);
        Date finalizeTime = Optional.ofNullable(responseContext.getDate()).orElse(DateUtil.DATE_ZERO);
        long responseTime = finalizeTime.equals(DateUtil.DATE_ZERO) ? 0 : finalizeTime.getTime() - accessTime.getTime();

        // Grab user info
        SecurityContext securityContext = requestContext.getSecurityContext();
        if (securityContext instanceof AuthenticationFilter.Authorizer) {
            AuthenticationFilter.Authorizer authorizer = (AuthenticationFilter.Authorizer)securityContext;
            userName = authorizer.username();
            userId = authorizer.userId();
            roles = authorizer.role().toString();
        }

        // build log string
        // TODO by Scorpiour: Use Formatted log template to replace hard-written string when logging
        LOG.info("{} /{} Status: {} - user: {} {} - roles: {} in {} ms", method, path, code, userId, userName, roles, responseTime);
    }
}
