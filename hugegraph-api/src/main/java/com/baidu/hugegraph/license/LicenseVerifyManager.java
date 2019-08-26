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

package com.baidu.hugegraph.license;

import java.lang.management.ManagementFactory;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.config.ServerOptions;
import com.baidu.hugegraph.util.Bytes;
import com.baidu.hugegraph.util.Log;
import com.sun.management.OperatingSystemMXBean;

import de.schlichtherle.license.LicenseContent;
import de.schlichtherle.license.LicenseContentException;
import de.schlichtherle.license.LicenseParam;

public class LicenseVerifyManager extends CommonLicenseManager {

    private static final Logger LOG = Log.logger(LicenseVerifyManager.class);

    private static final int NO_LIMIT = -1;

    private final HugeConfig config;
    private final MachineInfo machineInfo;

    public LicenseVerifyManager(LicenseParam param, HugeConfig config) {
        super(param);
        this.config = config;
        this.machineInfo = new MachineInfo();
    }

    @Override
    protected synchronized void validate(LicenseContent content)
                                         throws LicenseContentException {
        // call super validate firstly
        super.validate(content);

        // Verify the customized license parameters.
        @SuppressWarnings("unchecked")
        List<ExtraParam> extraParams = (List<ExtraParam>) content.getExtra();
        String serverId = this.getServerId();
        LOG.debug("server id is {}", serverId);
        ExtraParam param = this.findMatchParam(serverId, extraParams);
        if (param == null) {
            throw new LicenseContentException("The current server's id " +
                                              "is not authorized");
        }

        this.checkIp(param);
        this.checkMac(param);
        this.checkCpu(param);
        this.checkRam(param);
        this.checkThreads(param);
        this.checkMemory(param);
    }

    private String getServerId() {
        return this.config.get(ServerOptions.SERVER_ID);
    }

    private ExtraParam findMatchParam(String id, List<ExtraParam> extraParams) {
        for (ExtraParam param : extraParams) {
            if (param.getId().equals(id)) {
                return param;
            }
        }
        return null;
    }

    private void checkIp(ExtraParam param) throws LicenseContentException {
        String expectIp = param.getIp();
        if (StringUtils.isEmpty(expectIp)) {
            return;
        }
        List<String> actualIps = this.machineInfo.getIpAddress();
        if (!actualIps.contains(expectIp)) {
            throw new LicenseContentException(String.format(
                      "The server's ip %s doesn't match the authorized '%s'",
                      actualIps, expectIp));
        }
    }

    private void checkMac(ExtraParam param) throws LicenseContentException {
        String expectMac = param.getMac();
        if (StringUtils.isEmpty(expectMac)) {
            return;
        }
        List<String> actualMacs = this.machineInfo.getMacAddress();
        if (!actualMacs.contains(expectMac)) {
            throw new LicenseContentException(String.format(
                      "The server's mac %s doesn't match the authorized '%s'",
                      actualMacs, expectMac));
        }
    }

    private void checkCpu(ExtraParam param) throws LicenseContentException {
        int expectCpus = param.getCpu();
        if (expectCpus == NO_LIMIT) {
            return;
        }
        int actualCpus = Runtime.getRuntime().availableProcessors();
        if (actualCpus > expectCpus) {
            throw new LicenseContentException(String.format(
                      "The server's cpus '%s' exceeded the limit '%s'",
                      actualCpus, expectCpus));
        }
    }

    private void checkRam(ExtraParam param) throws LicenseContentException {
        // Unit MB
        int expectRam = param.getRam();
        if (expectRam == NO_LIMIT) {
            return;
        }
        OperatingSystemMXBean mxBean = (OperatingSystemMXBean) ManagementFactory
                                       .getOperatingSystemMXBean();
        long actualRam = mxBean.getTotalPhysicalMemorySize() / Bytes.MB;
        if (actualRam > expectRam) {
            throw new LicenseContentException(String.format(
                      "The server's ram(MB) '%s' exceeded the limit(MB) '%s'",
                      actualRam, expectRam));
        }
    }

    private void checkThreads(ExtraParam param) throws LicenseContentException {
        int expectThreads = param.getThreads();
        if (expectThreads == NO_LIMIT) {
            return;
        }
        int actualThreads = this.config.get(ServerOptions.MAX_WORKER_THREADS);
        if (actualThreads > expectThreads) {
            throw new LicenseContentException(String.format(
                      "The server's max threads '%s' exceeded limit '%s'",
                      actualThreads, expectThreads));
        }
    }

    private void checkMemory(ExtraParam param) throws LicenseContentException {
        // Unit MB
        int expectMemory = param.getMemory();
        if (expectMemory == NO_LIMIT) {
            return;
        }
        long actualMemory = Runtime.getRuntime().maxMemory() / Bytes.MB;
        if (actualMemory > expectMemory) {
            throw new LicenseContentException(String.format(
                      "The server's max memory(MB) '%s' exceeded the " +
                      "limit(MB) '%s'", actualMemory, expectMemory));
        }
    }
}
