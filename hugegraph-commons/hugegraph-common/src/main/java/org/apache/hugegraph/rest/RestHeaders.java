/*
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

package org.apache.hugegraph.rest;

import java.util.Date;
import java.util.Iterator;

import kotlin.Pair;

public class RestHeaders {

    public static final String CONTENT_TYPE = "Content-Type";

    public static final String CONTENT_ENCODING = "Content-Encoding";

    public static final String AUTHORIZATION = "Authorization";

    public static final String APPLICATION_JSON = "application/json";

    public static final String BEARER_PREFIX = "Bearer ";

    private final okhttp3.Headers.Builder headersBuilder;

    public RestHeaders() {
        this.headersBuilder = new okhttp3.Headers.Builder();
    }

    public static RestHeaders convertToRestHeaders(okhttp3.Headers headers) {
        RestHeaders restHeaders = new RestHeaders();

        if (headers != null) {
            Iterator<Pair<String, String>> iter = headers.iterator();
            while (iter.hasNext()) {
                Pair<String, String> pair = iter.next();
                restHeaders.add(pair.getFirst(), pair.getSecond());
            }
        }
        return restHeaders;
    }

    public String get(String key) {
        return this.headersBuilder.get(key);
    }

    public Date getDate(String key) {
        return this.headersBuilder.build().getDate(key);
    }

    public RestHeaders add(String key, String value) {
        this.headersBuilder.add(key, value);
        return this;
    }

    public RestHeaders add(String key, Date value) {
        this.headersBuilder.add(key, value);
        return this;
    }

    @Override
    public int hashCode() {
        return this.toOkHttpHeader().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof RestHeaders) {
            return this.toOkHttpHeader().equals(((RestHeaders) obj).toOkHttpHeader());
        }
        return false;
    }

    public okhttp3.Headers toOkHttpHeader() {
        return this.headersBuilder.build();
    }
}
