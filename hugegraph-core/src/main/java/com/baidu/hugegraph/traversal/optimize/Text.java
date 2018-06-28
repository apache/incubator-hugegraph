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

package com.baidu.hugegraph.traversal.optimize;

import java.util.function.BiPredicate;

import org.apache.tinkerpop.gremlin.process.traversal.P;

import com.baidu.hugegraph.backend.query.Condition.RelationType;

public class Text extends P<Object> {

    private static final long serialVersionUID = -4775814319848365698L;

    private Text(final BiPredicate<Object, Object> predicate, String value) {
        super(predicate, value);
    }

    public static Text contains(String value) {
        return new Text(RelationType.TEXT_CONTAINS, value);
    }
}
