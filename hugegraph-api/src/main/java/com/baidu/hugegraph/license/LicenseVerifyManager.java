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
import com.sun.management.OperatingSystemMXBean;

public class LicenseVerifyManager {

    private static final Logger LOG = Log.logger(LicenseVerifyManager.class);

    private final LicenseManager licenseManager;
    private final MachineInfo machineInfo;

    private HugeConfig config;
    private GraphManager graphManager;

    public LicenseVerifyManager(LicenseInstallParam param) {
        this.licenseManager = LicenseManagerFactory.create(param,
                                                           this::validate);
        this.machineInfo = new MachineInfo();
    }

    public LicenseManager licenseManager() {
        return this.licenseManager;
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

    private synchronized void validate(LicenseParams params) {
        // Verify the customized license parameters.
        String serverId = this.getServerId();
        LOG.debug("Verify server id '{}'", serverId);
        LicenseExtraParam param = params.matchParam(serverId);
        if (param == null) {
            throw new HugeException("The current server id is not authorized");
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

    private void checkVersion(LicenseExtraParam param) {
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

    private void checkGraphs(LicenseExtraParam param) {
        int expectGraphs = param.graphs();
        if (expectGraphs == LicenseExtraParam.NO_LIMIT) {
            return;
        }
        int actualGraphs = this.graphManager().graphs().size();
        if (actualGraphs > expectGraphs) {
            throw newLicenseException(
                  "The server's graphs '%s' exceeded the authorized '%s'",
                  actualGraphs, expectGraphs);
        }
    }

    private void checkIpAndMac(LicenseExtraParam param) {
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

    private void checkCpu(LicenseExtraParam param) {
        int expectCpus = param.cpus();
        if (expectCpus == LicenseExtraParam.NO_LIMIT) {
            return;
        }
        int actualCpus = CoreOptions.CPUS;
        if (actualCpus > expectCpus) {
            throw newLicenseException(
                  "The server's cpus '%s' exceeded the limit '%s'",
                  actualCpus, expectCpus);
        }
    }

    private void checkRam(LicenseExtraParam param) {
        // Unit MB
        int expectRam = param.ram();
        if (expectRam == LicenseExtraParam.NO_LIMIT) {
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

    private void checkThreads(LicenseExtraParam param) {
        int expectThreads = param.threads();
        if (expectThreads == LicenseExtraParam.NO_LIMIT) {
            return;
        }
        int actualThreads = this.config().get(ServerOptions.MAX_WORKER_THREADS);
        if (actualThreads > expectThreads) {
            throw newLicenseException(
                  "The server's max threads '%s' exceeded limit '%s'",
                  actualThreads, expectThreads);
        }
    }

    private void checkMemory(LicenseExtraParam param) {
        // Unit MB
        int expectMemory = param.memory();
        if (expectMemory == LicenseExtraParam.NO_LIMIT) {
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
