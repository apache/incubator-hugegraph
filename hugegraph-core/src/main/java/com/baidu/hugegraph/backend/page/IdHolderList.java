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

import java.util.ArrayList;
import java.util.Collection;

import com.baidu.hugegraph.util.E;

public class IdHolderList extends ArrayList<IdHolder> {

    private static final IdHolderList EMPTY_P = new IdHolderList(true);
    private static final IdHolderList EMPTY_NP = new IdHolderList(false);

    private static final long serialVersionUID = -738694176552424990L;

    private final boolean paging;

    public static IdHolderList empty(boolean paging) {
        IdHolderList empty = paging ? EMPTY_P : EMPTY_NP;
        empty.clear();
        return empty;
    }

    public IdHolderList(boolean paging) {
        this.paging = paging;
    }

    public boolean paging() {
        return this.paging;
    }

    @Override
    public boolean add(IdHolder holder) {
        E.checkArgument(this.paging == holder.paging(),
                        "The IdHolder to be linked must be " +
                        "in same paging mode");
        return super.add(holder);
    }

    @Override
    public boolean addAll(Collection<? extends IdHolder> idHolders) {
        for (IdHolder idHolder : idHolders) {
            this.add(idHolder);
        }
        return true;
    }
}
