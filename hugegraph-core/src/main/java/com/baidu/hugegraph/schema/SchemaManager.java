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
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.baidu.hugegraph.schema;

import java.util.List;

import com.baidu.hugegraph.type.schema.EdgeLabel;
import com.baidu.hugegraph.type.schema.IndexLabel;
import com.baidu.hugegraph.type.schema.PropertyKey;
import com.baidu.hugegraph.type.schema.VertexLabel;


public interface SchemaManager {

    public PropertyKey makePropertyKey(String name);

    public VertexLabel makeVertexLabel(String name);

    public EdgeLabel makeEdgeLabel(String name);

    public IndexLabel makeIndexLabel(String name);

    public PropertyKey propertyKey(String name);

    public VertexLabel vertexLabel(String name);

    public EdgeLabel edgeLabel(String name);

    public IndexLabel indexLabel(String name);

    public List<SchemaElement> desc();

    public SchemaElement create(SchemaElement element);

    public SchemaElement append(SchemaElement element);

    public SchemaElement eliminate(SchemaElement element);

    public void remove(SchemaElement element);
}
