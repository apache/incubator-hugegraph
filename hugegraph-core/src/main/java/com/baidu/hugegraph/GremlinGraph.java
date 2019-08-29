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

package com.baidu.hugegraph;

import org.apache.tinkerpop.gremlin.structure.Graph;

import com.baidu.hugegraph.auth.UserManager;
import com.baidu.hugegraph.schema.SchemaManager;

/**
 * Graph interface for Gremlin operations
 */
public interface GremlinGraph extends Graph {

    public String name();

    public SchemaManager schema();

    public String matchUser(String username, String password);
    public UserManager userManager();

    public String backend();

    public void initBackend();
    public void clearBackend();
    public void truncateBackend();
}
