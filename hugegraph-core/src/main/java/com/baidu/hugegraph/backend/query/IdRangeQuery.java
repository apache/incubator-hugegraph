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

public final class IdRangeQuery extends Query {

    private final Id start;
    private final Id end;
    private final boolean inclusiveStart;
    private final boolean inclusiveEnd;

    public IdRangeQuery(HugeType resultType, Id start, Id end) {
        this(resultType, null, start, end);
    }

    public IdRangeQuery(HugeType resultType, Query originQuery,
                        Id start, Id end) {
        this(resultType, originQuery, start, true, end, false);
    }

    public IdRangeQuery(Query originQuery,
                        Id start, boolean inclusiveStart,
                        Id end, boolean inclusiveEnd) {
        this(originQuery.resultType(), originQuery,
             start, inclusiveStart, end, inclusiveEnd);
    }

    public IdRangeQuery(HugeType resultType, Query originQuery,
                        Id start, boolean inclusiveStart,
                        Id end, boolean inclusiveEnd) {
        super(resultType, originQuery);
        E.checkArgumentNotNull(start, "The start parameter can't be null");
        this.start = start;
        this.end = end;
        this.inclusiveStart = inclusiveStart;
        this.inclusiveEnd = inclusiveEnd;
    }

    public Id start() {
        return this.start;
    }

    public Id end() {
        return this.end;
    }

    public boolean inclusiveStart() {
        return this.inclusiveStart;
    }

    public boolean inclusiveEnd() {
        return this.inclusiveEnd;
    }

    @Override
    public boolean empty() {
        return false;
    }

    @Override
    public boolean test(HugeElement element) {
        int cmp1 = Bytes.compare(element.id().asBytes(), this.start.asBytes());
        int cmp2 = Bytes.compare(element.id().asBytes(), this.end.asBytes());
        return (this.inclusiveStart ? cmp1 >= 0 : cmp1 > 0) &&
               (this.inclusiveEnd ? cmp2 <= 0 : cmp2 < 0);
    }

    @Override
    public IdRangeQuery copy() {
        return (IdRangeQuery) super.copy();
    }

    @Override
    public String toString() {
        return String.format("%s where id in range %s%s,%s%s",
                             super.toString(),
                             this.inclusiveStart ? "[" : "(",
                             this.start, this.end,
                             this.inclusiveEnd ? "]" : ")");
    }
}
