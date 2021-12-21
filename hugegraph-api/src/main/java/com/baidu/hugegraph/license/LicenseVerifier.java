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
import java.io.InputStream;
import java.time.Duration;
import java.time.Instant;

import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.core.GraphManager;
import com.baidu.hugegraph.util.Log;
import com.fasterxml.jackson.databind.ObjectMapper;

public class LicenseVerifier {

    private static final Logger LOG = Log.logger(HugeGraph.class);

    private static final String LICENSE_PARAM_PATH = "/verify-license.json";

    private static volatile LicenseVerifier INSTANCE = null;

    private static final Duration CHECK_INTERVAL = Duration.ofMinutes(10);
    private volatile Instant lastCheckTime = Instant.now();

    private final LicenseInstallParam installParam;
    private final LicenseVerifyManager manager;

    private LicenseVerifier() {
        this.installParam = buildInstallParam(LICENSE_PARAM_PATH);
        this.manager = new LicenseVerifyManager(this.installParam);
    }

    public static LicenseVerifier instance() {
        if (INSTANCE == null) {
            synchronized(LicenseVerifier.class) {
                if (INSTANCE == null) {
                    INSTANCE = new LicenseVerifier();
                }
            }
        }
        return INSTANCE;
    }

    public synchronized void install(HugeConfig config,
                                     GraphManager graphManager,
                                     String md5) {
        this.manager.config(config);
        this.manager.graphManager(graphManager);
        LicenseManager licenseManager = this.manager.licenseManager();
        try {
            licenseManager.uninstallLicense();
            this.verifyPublicCert(md5);
            LicenseParams params = licenseManager.installLicense();
            LOG.info("The license '{}' is successfully installed for '{}', " +
                     "the term of validity is from {} to {}",
                     params.subject(), params.consumerType(),
                     params.notBefore(), params.notAfter());
        } catch (Exception e) {
            LOG.error("Failed to install license", e);
            throw new HugeException("Failed to install license", e);
        }
    }

    public void verifyIfNeeded() {
        Instant now = Instant.now();
        Duration interval = Duration.between(this.lastCheckTime, now);
        if (!interval.minus(CHECK_INTERVAL).isNegative()) {
            this.verify();
            this.lastCheckTime = now;
        }
    }

    public void verify() {
        try {
            LicenseParams params = this.manager.licenseManager()
                                               .verifyLicense();
            LOG.info("The license verification passed, " +
                     "the term of validity is from {} to {}",
                     params.notBefore(), params.notAfter());
        } catch (Exception e) {
            LOG.error("Failed to verify license", e);
            throw new HugeException("Failed to verify license", e);
        }
    }

    private void verifyPublicCert(String expectMD5) {
        String path = this.installParam.publicKeyPath();
        try (InputStream is = LicenseVerifier.class.getResourceAsStream(path)) {
            String actualMD5 = DigestUtils.md5Hex(is);
            if (!actualMD5.equals(expectMD5)) {
                throw new HugeException("Invalid public cert");
            }
        } catch (IOException e) {
            LOG.error("Failed to read public cert", e);
            throw new HugeException("Failed to read public cert", e);
        }
    }

    private static LicenseInstallParam buildInstallParam(String path) {
        // NOTE: can't use JsonUtil due to it bind tinkerpop jackson
        ObjectMapper mapper = new ObjectMapper();
        try (InputStream stream =
             LicenseVerifier.class.getResourceAsStream(path)) {
            return mapper.readValue(stream, LicenseInstallParam.class);
        } catch (IOException e) {
            throw new HugeException("Failed to read json stream to %s",
                                    LicenseInstallParam.class);
        }
    }
}
