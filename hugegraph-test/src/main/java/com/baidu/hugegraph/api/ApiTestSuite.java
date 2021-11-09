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

package com.baidu.hugegraph.api;

import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import com.baidu.hugegraph.api.traversers.TraversersApiTestSuite;
import com.baidu.hugegraph.dist.RegisterUtil;

@RunWith(Suite.class)
@Suite.SuiteClasses({
    PropertyKeyApiTest.class,
    VertexLabelApiTest.class,
    EdgeLabelApiTest.class,
    IndexLabelApiTest.class,
    SchemaApiTest.class,
    VertexApiTest.class,
    EdgeApiTest.class,
    TaskApiTest.class,
    GremlinApiTest.class,
    MetricsApiTest.class,
    UserApiTest.class,
    LoginApiTest.class,
    ProjectApiTest.class,
    TraversersApiTestSuite.class
})
public class ApiTestSuite {

    @BeforeClass
    public static void initEnv() {
        RegisterUtil.registerBackends();
    }
}
