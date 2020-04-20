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

package com.baidu.hugegraph.job.system;

import java.util.Set;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.tx.GraphTransaction;
import com.baidu.hugegraph.structure.HugeElement;
import com.baidu.hugegraph.util.E;

public class DeleteExpiredElementJob extends DeleteExpiredJob<Object> {

    private static final String JOB_TYPE = "delete_expired_element";

    private Set<HugeElement> elements;

    public DeleteExpiredElementJob(Set<HugeElement> elements) {
        E.checkArgument(elements != null && !elements.isEmpty(),
                        "The element can't be null or empty");
        this.elements = elements;
    }

    @Override
    public String type() {
        return JOB_TYPE;
    }

    @Override
    public Object execute() throws Exception {
        HugeGraph graph = this.graph();
        GraphTransaction tx = graph.graphTransaction();
        try {
            for (HugeElement element : this.elements) {
                element.remove();
            }
            tx.commit();
        } catch (Exception e) {
            tx.rollback();
            LOG.warn("Failed to delete expired elements: {}", this.elements);
            throw e;
        } finally {
            JOB_COUNTERS.jobCounter(graph).decrement();
        }
        return null;
    }
}
