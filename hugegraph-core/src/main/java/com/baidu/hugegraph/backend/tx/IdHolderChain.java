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

package com.baidu.hugegraph.backend.tx;

import java.util.ArrayList;
import java.util.List;

import com.baidu.hugegraph.util.E;

public class IdHolderChain {

    private final boolean paged;
    private final List<IdHolder> idHolders;

    public IdHolderChain(boolean paged) {
        this.paged = paged;
        this.idHolders = new ArrayList<>();
    }

    public boolean paged() {
        return this.paged;
    }

    public List<IdHolder> idsHolders() {
        return this.idHolders;
    }

    public IdHolderChain(IdHolder idHolder) {
        if (idHolder instanceof PagedIdHolder) {
            this.paged = true;
        } else {
            assert idHolder instanceof EntireIdHolder;
            this.paged = false;
        }
        this.idHolders = new ArrayList<>();
        this.idHolders.add(idHolder);
    }

    public void link(IdHolder idHolder) {
        this.checkIdsHolderType(idHolder);
        if (this.paged) {
            this.idHolders.add(idHolder);
        } else {
            if (this.idHolders.isEmpty()) {
                this.idHolders.add(idHolder);
            } else {
                IdHolder selfIdHolder = this.idHolders.get(0);
                assert selfIdHolder instanceof EntireIdHolder;
                EntireIdHolder holder = (EntireIdHolder) idHolder;
                ((EntireIdHolder) selfIdHolder).merge(holder);
            }
        }
    }

    public void link(List<IdHolder> idHolders) {
        for (IdHolder idHolder : idHolders) {
            this.link(idHolder);
        }
    }

    public void link(IdHolderChain chain) {
        E.checkArgument((this.paged && chain.paged) ||
                        (!this.paged && !chain.paged),
                        "Only same IdHolderChain can be linked");
        this.link(chain.idsHolders());
    }

    private void checkIdsHolderType(IdHolder idHolder) {
        if (this.paged) {
            E.checkArgument(idHolder instanceof PagedIdHolder,
                            "The IdHolder to be linked must be " +
                            "PagedIdHolder in paged mode");
        } else {
            E.checkArgument(idHolder instanceof EntireIdHolder,
                            "The IdHolder to be linked must be " +
                            "EntireIdHolder in non-paged mode");
        }
    }

    public long size() {
        if (this.paged) {
            return this.idHolders.size();
        } else {
            return this.idHolders.get(0).size();
        }
    }
}
