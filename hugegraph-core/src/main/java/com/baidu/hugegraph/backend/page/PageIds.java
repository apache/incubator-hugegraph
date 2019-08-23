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

package com.baidu.hugegraph.backend.page;

import java.util.Set;

import com.baidu.hugegraph.backend.id.Id;
import com.google.common.collect.ImmutableSet;

public final class PageIds {

    public static final PageIds EMPTY = new PageIds(ImmutableSet.of(),
                                                    (String) null);

    private final Set<Id> ids;
    private final String page;

    public PageIds(Set<Id> ids, String page) {
        this.ids = ids;
        this.page = page;
    }

    public PageIds(Set<Id> ids, PageState pageState) {
        this.ids = ids;
        this.page = pageState.toString();
    }

    public Set<Id> ids() {
        return this.ids;
    }

    public String page() {
        return this.page;
    }

    public boolean empty() {
        return this.ids.isEmpty();
    }
}
