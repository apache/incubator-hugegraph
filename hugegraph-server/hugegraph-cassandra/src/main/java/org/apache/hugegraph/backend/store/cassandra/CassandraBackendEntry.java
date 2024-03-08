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

package org.apache.hugegraph.backend.store.cassandra;

import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.serializer.TableBackendEntry;
import org.apache.hugegraph.type.HugeType;

public class CassandraBackendEntry extends TableBackendEntry {

    public CassandraBackendEntry(Id id) {
        super(id);
    }

    public CassandraBackendEntry(HugeType type) {
        this(type, null);
    }

    public CassandraBackendEntry(HugeType type, Id id) {
        this(new Row(type, id));
    }

    public CassandraBackendEntry(TableBackendEntry.Row row) {
        super(row);
    }
}
