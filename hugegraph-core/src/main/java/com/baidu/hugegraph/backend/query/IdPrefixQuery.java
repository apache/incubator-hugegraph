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

package com.baidu.hugegraph.backend.query;

import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.structure.HugeElement;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.util.Bytes;
import com.baidu.hugegraph.util.E;

public class IdPrefixQuery extends Query {

    private final Id start;
    private final boolean inclusiveStart;
    private final Id prefix;

    public IdPrefixQuery(HugeType resultType, Id prefix) {
        this(resultType, null, prefix, true, prefix);
    }

    public IdPrefixQuery(Query originQuery, Id prefix) {
        this(originQuery.resultType(), originQuery, prefix, true, prefix);
    }

    public IdPrefixQuery(Query originQuery,
                         Id start, boolean inclusive, Id prefix) {
        this(originQuery.resultType(), originQuery, start, inclusive, prefix);
    }

    public IdPrefixQuery(HugeType resultType, Query originQuery,
                         Id start, boolean inclusive, Id prefix) {
        super(resultType, originQuery);
        E.checkArgumentNotNull(start, "The start parameter can't be null");
        this.start = start;
        this.inclusiveStart = inclusive;
        this.prefix = prefix;
    }

    public Id start() {
        return this.start;
    }

    public boolean inclusiveStart() {
        return this.inclusiveStart;
    }

    public Id prefix() {
        return this.prefix;
    }

    @Override
    public boolean empty() {
        return false;
    }

    @Override
    public boolean test(HugeElement element) {
        byte[] elem = element.id().asBytes();
        int cmp = Bytes.compare(elem, this.start.asBytes());
        boolean matchedStart = this.inclusiveStart ? cmp >= 0 : cmp > 0;
        boolean matchedPrefix = Bytes.prefixWith(elem, this.prefix.asBytes());
        return matchedStart && matchedPrefix;
    }

    @Override
    public IdPrefixQuery copy() {
        return (IdPrefixQuery) super.copy();
    }

    @Override
    public String toString() {
        return String.format("%s where id prefix with %s and start with %s(%s)",
                             super.toString(), this.prefix, this.start,
                             this.inclusiveStart ? "inclusive" : "exclusive");
    }
}
