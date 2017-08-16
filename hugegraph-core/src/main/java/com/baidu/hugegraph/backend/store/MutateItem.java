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
package com.baidu.hugegraph.backend.store;


public class MutateItem {

    private BackendEntry entry;
    private MutateAction action;

    public static MutateItem of(BackendEntry entry, MutateAction action) {
        return new MutateItem(entry, action);
    }

    public MutateItem(BackendEntry entry, MutateAction action) {
        this.entry = entry;
        this.action = action;
    }

    public BackendEntry entry() {
        return this.entry;
    }

    public void entry(BackendEntry entry) {
        this.entry = entry;
    }

    public MutateAction action() {
        return this.action;
    }

    public void action(MutateAction action) {
        this.action = action;
    }

    @Override
    public String toString() {
        return String.format("entry: %s, action: %s", this.entry, this.action);
    }
}
