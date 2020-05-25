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

import com.baidu.hugegraph.analyzer.Analyzer;
import com.baidu.hugegraph.backend.serializer.AbstractSerializer;
import com.baidu.hugegraph.backend.store.BackendStore;
import com.baidu.hugegraph.backend.tx.GraphTransaction;
import com.baidu.hugegraph.backend.tx.SchemaTransaction;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.event.EventHub;
import com.baidu.hugegraph.type.define.GraphMode;
import com.google.common.util.concurrent.RateLimiter;

/**
 * Graph inner Params interface
 */
public interface HugeGraphParams {

    public HugeGraph graph();
    public String name();
    public GraphMode mode();

    public SchemaTransaction schemaTransaction();
    public GraphTransaction systemTransaction();
    public GraphTransaction graphTransaction();

    public GraphTransaction openTransaction();
    public void closeTx();

    public BackendStore loadSchemaStore();
    public BackendStore loadGraphStore();
    public BackendStore loadSystemStore();

    public EventHub schemaEventHub();
    public EventHub graphEventHub();
    public EventHub indexEventHub();

    public HugeConfig configuration();

    public AbstractSerializer serializer();
    public Analyzer analyzer();
    public RateLimiter rateLimiter();
}
