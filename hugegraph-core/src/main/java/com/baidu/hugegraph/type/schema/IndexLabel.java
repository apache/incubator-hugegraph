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

package com.baidu.hugegraph.type.schema;

import java.util.List;

import com.baidu.hugegraph.schema.SchemaElement;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.IndexType;


public abstract class IndexLabel extends SchemaElement {

    public IndexLabel(String name) {
        super(name);
    }

    @Override
    public HugeType type() {
        return HugeType.INDEX_LABEL;
    }

    public abstract IndexLabel on(SchemaElement element);

    public abstract IndexLabel by(String... indexFields);

    public abstract IndexLabel secondary();

    public abstract IndexLabel search();

    public abstract IndexLabel create();

    public abstract HugeType baseType();

    public abstract String baseValue();

    public abstract IndexType indexType();

    public abstract HugeType queryType();

    public abstract List<String> indexFields();

    public abstract void rebuild();

    @Override
    public IndexLabel ifNotExist() {
        this.checkExist = false;
        return this;
    }
}
