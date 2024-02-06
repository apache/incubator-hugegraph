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

package org.apache.hugegraph.backend.query;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.structure.HugeElement;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.InsertionOrderUtil;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

public class IdQuery extends Query {

    private static final List<Id> EMPTY_IDS = ImmutableList.of();

    // The id(s) will be concated with `or`
    private List<Id> ids = EMPTY_IDS;
    private boolean mustSortByInput = true;

    public IdQuery(HugeType resultType) {
        super(resultType);
    }

    public IdQuery(HugeType resultType, Query originQuery) {
        super(resultType, originQuery);
    }

    public IdQuery(HugeType resultType, Set<Id> ids) {
        this(resultType);
        this.query(ids);
    }

    public IdQuery(HugeType resultType, Id id) {
        this(resultType);
        this.query(id);
    }

    public IdQuery(Query originQuery, Id id) {
        this(originQuery.resultType(), originQuery);
        this.query(id);
    }

    public IdQuery(Query originQuery, Set<Id> ids) {
        this(originQuery.resultType(), originQuery);
        this.query(ids);
    }

    public boolean mustSortByInput() {
        return this.mustSortByInput;
    }

    public void mustSortByInput(boolean mustSortedByInput) {
        this.mustSortByInput = mustSortedByInput;
    }

    @Override
    public int idsSize() {
        return this.ids.size();
    }

    @Override
    public Collection<Id> ids() {
        return Collections.unmodifiableList(this.ids);
    }

    public void resetIds() {
        this.ids = EMPTY_IDS;
    }

    public IdQuery query(Id id) {
        E.checkArgumentNotNull(id, "Query id can't be null");
        if (this.ids == EMPTY_IDS) {
            this.ids = InsertionOrderUtil.newList();
        }

        int last = this.ids.size() - 1;
        if (last >= 0 && id.equals(this.ids.get(last))) {
            // The same id as the previous one, just ignore it
            return this;
        }

        this.ids.add(id);
        this.checkCapacity(this.ids.size());
        return this;
    }

    public IdQuery query(Set<Id> ids) {
        for (Id id : ids) {
            this.query(id);
        }
        return this;
    }

    @Override
    public boolean test(HugeElement element) {
        return this.ids.contains(element.id());
    }

    @Override
    public IdQuery copy() {
        IdQuery query = (IdQuery) super.copy();
        query.ids = this.ids == EMPTY_IDS ? EMPTY_IDS :
                    InsertionOrderUtil.newList(this.ids);
        return query;
    }

    public static final class OneIdQuery extends IdQuery {

        private Id id;

        public OneIdQuery(HugeType resultType, Id id) {
            super(resultType);
            super.mustSortByInput = false;
            this.id = id;
        }

        public OneIdQuery(Query originQuery, Id id) {
            super(originQuery.resultType(), originQuery);
            super.mustSortByInput = false;
            this.id = id;
        }

        public Id id() {
            return this.id;
        }

        public void resetId(Id id) {
            this.id = id;
        }

        @Override
        public int idsSize() {
            return this.id == null ? 0 : 1;
        }

        @Override
        public Set<Id> ids() {
            return this.id == null ? ImmutableSet.of() :
                                     ImmutableSet.of(this.id);
        }

        @Override
        public void resetIds() {
            this.id = null;
        }

        @Override
        public IdQuery query(Id id) {
            E.checkArgumentNotNull(id, "Query id can't be null");
            this.id = id;
            return this;
        }

        @Override
        public boolean test(HugeElement element) {
            if (this.id == null) {
                return true;
            }
            return this.id.equals(element.id());
        }

        @Override
        public IdQuery copy() {
            OneIdQuery query = (OneIdQuery) super.copy();
            assert this.id.equals(query.id);
            return query;
        }
    }
}
