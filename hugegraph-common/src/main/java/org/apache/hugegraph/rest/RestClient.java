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

package org.apache.hugegraph.rest;

import java.util.Map;

import jakarta.ws.rs.core.MultivaluedMap;

public interface RestClient {
    /**
     * Post method
     */
    RestResult post(String path, Object object);

    RestResult post(String path, Object object, MultivaluedMap<String, Object> headers);

    RestResult post(String path, Object object, Map<String, Object> params);

    RestResult post(String path, Object object, MultivaluedMap<String, Object> headers,
                    Map<String, Object> params);

    /**
     * Put method
     */
    RestResult put(String path, String id, Object object);

    RestResult put(String path, String id, Object object, MultivaluedMap<String, Object> headers);

    RestResult put(String path, String id, Object object, Map<String, Object> params);

    RestResult put(String path, String id, Object object, MultivaluedMap<String, Object> headers,
                   Map<String, Object> params);

    /**
     * Get method
     */
    RestResult get(String path);

    RestResult get(String path, Map<String, Object> params);

    RestResult get(String path, String id);

    /**
     * Delete method
     */
    RestResult delete(String path, Map<String, Object> params);

    RestResult delete(String path, String id);

    void close();
}
