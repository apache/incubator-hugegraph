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

package org.apache.hugegraph.job.algorithm.comm;

import java.util.Map;

import org.apache.hugegraph.job.UserJob;
import org.apache.hugegraph.traversal.algorithm.HugeTraverser;
import org.apache.hugegraph.util.ParameterUtil;

public class LouvainAlgorithm extends AbstractCommAlgorithm {

    public static final String ALGO_NAME = "louvain";

    @Override
    public String name() {
        return ALGO_NAME;
    }

    @Override
    public void checkParameters(Map<String, Object> parameters) {
        times(parameters);
        stableTimes(parameters);
        precision(parameters);
        degree(parameters);
        sourceLabel(parameters);
        sourceCLabel(parameters);
        showModularity(parameters);
        showCommunity(parameters);
        exportCommunity(parameters);
        skipIsolated(parameters);
        clearPass(parameters);
        workers(parameters);
    }

    @Override
    public Object call(UserJob<Object> job, Map<String, Object> parameters) {
        String label = sourceLabel(parameters);
        String clabel = sourceCLabel(parameters);
        long degree = degree(parameters);
        boolean skipIsolated = skipIsolated(parameters);
        int workers = workers(parameters);

        Long clearPass = clearPass(parameters);
        Long modPass = showModularity(parameters);
        String showComm = showCommunity(parameters);
        Long exportPass = exportCommunity(parameters);

        try (LouvainTraverser traverser = new LouvainTraverser(
                                          job, workers, degree,
                                          label, clabel, skipIsolated)) {
            if (clearPass != null) {
                return traverser.clearPass(clearPass.intValue());
            } else if (modPass != null) {
                return traverser.modularity(modPass.intValue());
            } else if (exportPass != null) {
                boolean vertexFirst = showComm == null;
                int pass = exportPass.intValue();
                return traverser.exportCommunity(pass, vertexFirst);
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
        long pass = ParameterUtil.parameterLong(parameters, KEY_CLEAR);
        HugeTraverser.checkNonNegativeOrNoLimit(pass, KEY_CLEAR);
        return pass;
    }

    protected static Long showModularity(Map<String, Object> parameters) {
        if (!parameters.containsKey(KEY_SHOW_MOD)) {
            return null;
        }
        long pass = ParameterUtil.parameterLong(parameters, KEY_SHOW_MOD);
        HugeTraverser.checkNonNegative(pass, KEY_SHOW_MOD);
        return pass;
    }

    protected static Long exportCommunity(Map<String, Object> parameters) {
        if (!parameters.containsKey(KEY_EXPORT_COMM)) {
            return null;
        }
        long pass = ParameterUtil.parameterLong(parameters, KEY_EXPORT_COMM);
        HugeTraverser.checkNonNegative(pass, KEY_EXPORT_COMM);
        return pass;
    }

    protected static boolean skipIsolated(Map<String, Object> parameters) {
        if (!parameters.containsKey(KEY_SKIP_ISOLATED)) {
            return true;
        }
        return ParameterUtil.parameterBoolean(parameters, KEY_SKIP_ISOLATED);
    }
}
