/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hugegraph.backend.store;

import org.apache.hugegraph.type.define.Action;

public class BackendAction {

    private final Action action;
    private final BackendEntry entry;

    public static BackendAction of(Action action, BackendEntry entry) {
        return new BackendAction(entry, action);
    }

    public BackendAction(BackendEntry entry, Action action) {
        this.action = action;
        this.entry = entry;
    }

    public Action action() {
        return this.action;
    }

    public BackendEntry entry() {
        return this.entry;
    }

    @Override
    public String toString() {
        return String.format("entry: %s, action: %s", this.entry, this.action);
    }
}
