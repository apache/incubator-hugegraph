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

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.config.CoreOptions;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.config.ServerOptions;
import com.baidu.hugegraph.core.GraphManager;
import com.baidu.hugegraph.util.Bytes;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;
import com.baidu.hugegraph.util.VersionUtil;
import com.baidu.hugegraph.version.CoreVersion;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.management.OperatingSystemMXBean;

import de.schlichtherle.license.LicenseContent;
import de.schlichtherle.license.LicenseContentException;
import de.schlichtherle.license.LicenseParam;

public class LicenseVerifyManager extends CommonLicenseManager {

    private static final Logger LOG = Log.logger(LicenseVerifyManager.class);

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final int NO_LIMIT = -1;

    private HugeConfig config;
    private GraphManager graphManager;
    private final MachineInfo machineInfo;

    public LicenseVerifyManager(LicenseParam param) {
        super(param);
        this.machineInfo = new MachineInfo();
    }

    public void config(HugeConfig config) {
        this.config = config;
    }

    public HugeConfig config() {
        E.checkState(this.config != null,
                     "License verify manager has not been installed");
        return this.config;
    }

    public void graphManager(GraphManager graphManager) {
        this.graphManager = graphManager;
    }

    public GraphManager graphManager() {
        E.checkState(this.graphManager != null,
                     "License verify manager has not been installed");
        return this.graphManager;
    }

    @Override
    protected synchronized void validate(LicenseContent content) {
        // call super validate firstly
        try {
            super.validate(content);
        } catch (LicenseContentException e) {
            throw new HugeException("Failed to verify license", e);
        }

        // Verify the customized license parameters.
        List<ExtraParam> extraParams;
        try {
            TypeReference<List<ExtraParam>> type;
            type = new TypeReference<List<ExtraParam>>() {};
            extraParams = MAPPER.readValue((String) content.getExtra(), type);
        } catch (IOException e) {
            throw new RuntimeException("Failed to read extra params", e);
        }

        String serverId = this.getServerId();
        LOG.debug("server id is {}", serverId);
        ExtraParam param = this.matchParam(serverId, extraParams);
        if (param == null) {
            throw new HugeException("The current server's id is not authorized");
        }

        this.checkVersion(param);
        this.checkGraphs(param);
        this.checkIpAndMac(param);
        this.checkCpu(param);
        this.checkRam(param);
        this.checkThreads(param);
        this.checkMemory(param);
    }

    private String getServerId() {
        return this.config().get(ServerOptions.SERVER_ID);
    }

    private ExtraParam matchParam(String id, List<ExtraParam> extraParams) {
        for (ExtraParam param : extraParams) {
            if (param.id().equals(id)) {
                return param;
            }
        }
        return null;
    }

    private void checkVersion(ExtraParam param) {
        String expectVersion = param.version();
        if (StringUtils.isEmpty(expectVersion)) {
            return;
        }
        VersionUtil.Version acutalVersion = CoreVersion.VERSION;
        if (acutalVersion.compareTo(VersionUtil.Version.of(expectVersion)) > 0) {
            throw newLicenseException(
                  "The server's version '%s' exceeded the authorized '%s'",
                  acutalVersion.get(), expectVersion);
        }
    }

    private void checkGraphs(ExtraParam param) {
        int expectGraphs = param.graphs();
        if (expectGraphs == NO_LIMIT) {
            return;
        }
        int actualGraphs = this.graphManager().graphs().size();
        if (actualGraphs > expectGraphs) {
            throw newLicenseException(
                  "The server's graphs '%s' exceeded the authorized '%s'",
                  actualGraphs, expectGraphs);
        }
    }

    private void checkIpAndMac(ExtraParam param) {
        String expectIp = param.ip();
        boolean matched = false;
        List<String> actualIps = this.machineInfo.getIpAddress();
        for (String actualIp : actualIps) {
            if (StringUtils.isEmpty(expectIp) ||
                actualIp.equalsIgnoreCase(expectIp)) {
                matched = true;
                break;
            }
        }
        if (!matched) {
            throw newLicenseException(
                  "The server's ip '%s' doesn't match the authorized '%s'",
                  actualIps, expectIp);
        }

        String expectMac = param.mac();
        if (StringUtils.isEmpty(expectMac)) {
            return;
        }

        // The mac must be not empty here
        if (!StringUtils.isEmpty(expectIp)) {
            String actualMac;
            try {
                actualMac = this.machineInfo.getMacByInetAddress(
                            InetAddress.getByName(expectIp));
            } catch (UnknownHostException e) {
                throw newLicenseException(
                      "Failed to get mac address for ip '%s'", expectIp);
            }
            String expectFormatMac = expectMac.replaceAll(":", "-");
            String actualFormatMac = actualMac.replaceAll(":", "-");
            if (!actualFormatMac.equalsIgnoreCase(expectFormatMac)) {
                throw newLicenseException(
                      "The server's mac '%s' doesn't match the authorized '%s'",
                      actualMac, expectMac);
            }
        } else {
            String expectFormatMac = expectMac.replaceAll(":", "-");
            List<String> actualMacs = this.machineInfo.getMacAddress();
            matched = false;
            for (String actualMac : actualMacs) {
                String actualFormatMac = actualMac.replaceAll(":", "-");
                if (actualFormatMac.equalsIgnoreCase(expectFormatMac)) {
                    matched = true;
                    break;
                }
            }
            if (!matched) {
                throw newLicenseException(
                      "The server's macs %s don't match the authorized '%s'",
                      actualMacs, expectMac);
            }
        }
    }

    private void checkCpu(ExtraParam param) {
        int expectCpus = param.cpus();
        if (expectCpus == NO_LIMIT) {
            return;
        }
        int actualCpus = CoreOptions.CPUS;
        if (actualCpus > expectCpus) {
            throw newLicenseException(
                  "The server's cpus '%s' exceeded the limit '%s'",
                  actualCpus, expectCpus);
        }
    }

    private void checkRam(ExtraParam param) {
        // Unit MB
        int expectRam = param.ram();
        if (expectRam == NO_LIMIT) {
            return;
        }
        OperatingSystemMXBean mxBean = (OperatingSystemMXBean) ManagementFactory
                                       .getOperatingSystemMXBean();
        long actualRam = mxBean.getTotalPhysicalMemorySize() / Bytes.MB;
        if (actualRam > expectRam) {
            throw newLicenseException(
                  "The server's ram(MB) '%s' exceeded the limit(MB) '%s'",
                  actualRam, expectRam);
        }
    }

    private void checkThreads(ExtraParam param) {
        int expectThreads = param.threads();
        if (expectThreads == NO_LIMIT) {
            return;
        }
        int actualThreads = this.config().get(ServerOptions.MAX_WORKER_THREADS);
        if (actualThreads > expectThreads) {
            throw newLicenseException(
                  "The server's max threads '%s' exceeded limit '%s'",
                  actualThreads, expectThreads);
        }
    }

    private void checkMemory(ExtraParam param) {
        // Unit MB
        int expectMemory = param.memory();
        if (expectMemory == NO_LIMIT) {
            return;
        }
        /*
         * NOTE: this max memory will be slightly smaller than XMX,
         * because only one survivor will be used
         */
        long actualMemory = Runtime.getRuntime().maxMemory() / Bytes.MB;
        if (actualMemory > expectMemory) {
            throw newLicenseException(
                  "The server's max heap memory(MB) '%s' exceeded the " +
                  "limit(MB) '%s'", actualMemory, expectMemory);
        }
    }

    private HugeException newLicenseException(String message, Object... args) {
        return new HugeException(message, args);
    }
}
