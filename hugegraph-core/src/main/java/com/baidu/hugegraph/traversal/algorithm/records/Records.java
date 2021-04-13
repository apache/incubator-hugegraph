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

package com.baidu.hugegraph.traversal.algorithm.records;

import java.util.function.Function;

import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.traversal.algorithm.HugeTraverser.PathSet;

public interface Records {

    static final int INIT_CAPACITY = 16;

    public void startOneLayer(boolean forward);

    public void finishOneLayer();

    public boolean hasNextKey();

    public Id nextKey();

    public PathSet findPath(Id target, Function<Id, Boolean> filter,
                            boolean all, boolean ring);

    public long accessed();
}
