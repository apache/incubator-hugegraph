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

package org.apache.hugegraph.pd.rest.interceptor;

import java.io.IOException;
import java.util.function.Function;
import java.util.function.Supplier;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hugegraph.pd.rest.API;
import org.apache.hugegraph.pd.service.interceptor.Authentication;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Service;
import org.springframework.web.servlet.HandlerInterceptor;
import org.springframework.web.servlet.ModelAndView;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class RestAuthentication extends Authentication implements HandlerInterceptor {

    private static final String TOKEN_KEY = "Pd-Token";
    private static final Supplier<Boolean> DEFAULT_HANDLE = () -> true;

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response,
                             Object handler) throws
                                             IOException {
        try {
            String token = request.getHeader(TOKEN_KEY);
            String authority = request.getHeader("Authorization");

            if (authority == null) {
                throw new Exception("Unauthorized!");
            }

            Function<String, Boolean> tokenCall = t -> {
                response.addHeader(TOKEN_KEY, t);
                return true;
            };
            authority = authority.replace("Basic ", "");
            return authenticate(authority, token, tokenCall, DEFAULT_HANDLE);
        } catch (Exception e) {
            response.setContentType("application/json");
            response.getWriter().println(new API().toJSON(e));
            response.getWriter().flush();
            return false;
        }
    }

    @Override
    public void postHandle(HttpServletRequest request, HttpServletResponse response, Object handler,
                           @Nullable
                           ModelAndView modelAndView) {
    }

    @Override
    public void afterCompletion(HttpServletRequest request, HttpServletResponse response,
                                Object handler,
                                @Nullable Exception ex) {
    }
}
