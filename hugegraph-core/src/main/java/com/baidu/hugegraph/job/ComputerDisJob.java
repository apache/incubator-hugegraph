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

package com.baidu.hugegraph.job;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.computer.driver.JobStatus;
import com.baidu.hugegraph.job.computer.Computer;
import com.baidu.hugegraph.job.computer.ComputerPool;
import com.baidu.hugegraph.k8s.K8sDriverProxy;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.JsonUtil;
import com.baidu.hugegraph.util.Log;
import org.slf4j.Logger;

import java.util.HashMap;
import java.util.Map;

public class ComputerDisJob extends SysJob<Object> {

    private static final Logger LOG = Log.logger(ComputerDisJob.class);

    public static final String COMPUTER_DIS = "computer-dis";
    public static final String INNER_STATUS = "inner.status";
    public static final String INNER_JOB_ID = "inner.job.id";

    public static boolean check(String name, Map<String, Object> parameters) {
        Computer computer = ComputerPool.instance().find(name);
        if (computer == null) {
            return false;
        }
        computer.checkParameters(parameters);
        return true;
    }

    @Override
    public String type() {
        return COMPUTER_DIS;
    }

    @Override
    protected void cancelled() {
        super.cancelled();
        String input = this.task().input();
        E.checkArgumentNotNull(input, "The input can't be null");
        @SuppressWarnings("unchecked")
        Map<String, Object> map = JsonUtil.fromJson(input, Map.class);
        String algorithm = map.get("algorithm").toString();
        String graph = map.get("graph").toString();
        String token = map.get("token").toString();
        int worker = Integer.valueOf(map.get("worker").toString());
        Object value = map.get("params");
        E.checkArgument(value instanceof Map,
                        "Invalid computer parameters '%s'",
                        value);
        @SuppressWarnings("unchecked")
        Map<String, Object> params = (Map<String, Object>) value;
        Map<String, String> k8sParams = new HashMap<>();
        for (Map.Entry<String, Object> item : params.entrySet()) {
            k8sParams.put(item.getKey(), item.getValue().toString());
        }

        k8sParams.put("hugegraph.name", graph);
        k8sParams.put("hugegraph.token", token);
        k8sParams.put("k8s.worker_instances", String.valueOf(worker));
        if (map.containsKey(INNER_JOB_ID)) {
            String jobId = (String) map.get(INNER_JOB_ID);
            K8sDriverProxy k8sDriverProxy =
                           new K8sDriverProxy(String.valueOf(worker * 2),
                                              algorithm);
            k8sDriverProxy.getKubernetesDriver().cancelJob(jobId, k8sParams);
            k8sDriverProxy.close();
        }
    }

    @Override
    public Object execute() throws Exception {
        String input = this.task().input();
        E.checkArgumentNotNull(input, "The input can't be null");
        @SuppressWarnings("unchecked")
        Map<String, Object> map = JsonUtil.fromJson(input, Map.class);
        String status = map.containsKey(INNER_STATUS) ?
               map.get(INNER_STATUS).toString() : null;
        String jobId = map.containsKey(INNER_JOB_ID) ?
               map.get(INNER_JOB_ID).toString() : null;
        Object value = map.get("params");
        E.checkArgument(value instanceof Map,
                        "Invalid computer parameters '%s'",
                        value);
        @SuppressWarnings("unchecked")
        Map<String, Object> params = (Map<String, Object>) value;
        String algorithm = map.get("algorithm").toString();
        String graph = map.get("graph").toString();
        String token = map.get("token").toString();
        int worker = Integer.valueOf(map.get("worker").toString());

        Map<String, String> k8sParams = new HashMap<>();
        for (Map.Entry<String, Object> item : params.entrySet()) {
            k8sParams.put(item.getKey(), item.getValue().toString());
        }
        k8sParams.put("hugegraph.name", graph);
        k8sParams.put("hugegraph.token", token);
        k8sParams.put("k8s.worker_instances", String.valueOf(worker));
        if (status == null) {
            // TODO: DO TASK
            K8sDriverProxy k8sDriverProxy =
                           new K8sDriverProxy(String.valueOf(worker * 2),
                                              algorithm);
            if (jobId == null) {
                jobId = k8sDriverProxy.getKubernetesDriver()
                                      .submitJob(algorithm, k8sParams);
                map = JsonUtil.fromJson(this.task().input(), Map.class);
                map.put(INNER_JOB_ID, jobId);
                this.task().input(JsonUtil.toJson(map));
            }

            k8sDriverProxy.getKubernetesDriver()
                          .waitJob(jobId, k8sParams, observer -> {
                JobStatus jobStatus = observer.jobStatus();
                Map<String, Object> innerMap = JsonUtil.fromJson(
                                    this.task().input(), Map.class);
                innerMap.put(INNER_STATUS, jobStatus);
                this.task().input(JsonUtil.toJson(innerMap));
            });
            k8sDriverProxy.close();
        }

        map = JsonUtil.fromJson(this.task().input(), Map.class);
        status = map.get(INNER_STATUS).toString();
        if (status != null && status.equals("FAILED")) {
            throw new Exception("Computer-dis job failed.");
        }
        return status;
    }
}
