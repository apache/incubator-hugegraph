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

package com.baidu.hugegraph.backend.serializer;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.schema.EdgeLabel;
import com.baidu.hugegraph.schema.IndexLabel;
import com.baidu.hugegraph.schema.PropertyKey;
import com.baidu.hugegraph.schema.VertexLabel;

public interface SchemaSerializer {

    BackendEntry writeVertexLabel(VertexLabel vertexLabel);

    VertexLabel readVertexLabel(HugeGraph graph, BackendEntry entry);

    BackendEntry writeEdgeLabel(EdgeLabel edgeLabel);

    EdgeLabel readEdgeLabel(HugeGraph graph, BackendEntry entry);

    BackendEntry writePropertyKey(PropertyKey propertyKey);

    PropertyKey readPropertyKey(HugeGraph graph, BackendEntry entry);

    BackendEntry writeIndexLabel(IndexLabel indexLabel);

    IndexLabel readIndexLabel(HugeGraph graph, BackendEntry entry);
}
