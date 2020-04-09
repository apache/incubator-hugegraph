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

package com.baidu.hugegraph.job.algorithm.comm;

import java.util.Map;

import com.baidu.hugegraph.job.Job;
import com.baidu.hugegraph.util.E;

public class LouvainAlgorithm extends AbstractCommAlgorithm {

    @Override
    public String name() {
        return "louvain";
    }

    @Override
    public void checkParameters(Map<String, Object> parameters) {
        times(parameters);
        stableTimes(parameters);
        precision(parameters);
        degree(parameters);
        sourceLabel(parameters);
        sourceCLabel(parameters);
        showCommunity(parameters);
        clearPass(parameters);
    }

    @Override
    public Object call(Job<Object> job, Map<String, Object> parameters) {
        String label = sourceLabel(parameters);
        String clabel = sourceCLabel(parameters);
        long degree = degree(parameters);

        LouvainTraverser traverser = new LouvainTraverser(job, degree,
                                                          label, clabel);
        Long clearPass = clearPass(parameters);
        String showComm = showCommunity(parameters);
        try {
            if (clearPass != null) {
                return traverser.clearPass(clearPass.intValue());
            } else if (showComm != null) {
                return traverser.showCommunity(showComm);
            } else {
                return traverser.louvain(times(parameters),
                                         stableTimes(parameters),
                                         precision(parameters));
            }
        } catch (Throwable e) {
            job.graph().tx().rollback();
            throw e;
        }
    }

    protected static Long clearPass(Map<String, Object> parameters) {
        if (!parameters.containsKey(KEY_CLEAR)) {
            return null;
        }
        long pass = parameterLong(parameters, KEY_CLEAR);
        // TODO: change to checkNonNegative()
        E.checkArgument(pass >= 0 || pass == -1,
                        "The %s parameter must be >= 0 or == -1, but got %s",
                        KEY_CLEAR, pass);
        return pass;
    }
}
