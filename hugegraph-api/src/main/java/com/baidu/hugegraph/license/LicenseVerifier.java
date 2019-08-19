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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.prefs.Preferences;

import org.slf4j.Logger;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;
import com.fasterxml.jackson.databind.ObjectMapper;

import de.schlichtherle.license.CipherParam;
import de.schlichtherle.license.DefaultCipherParam;
import de.schlichtherle.license.DefaultKeyStoreParam;
import de.schlichtherle.license.DefaultLicenseParam;
import de.schlichtherle.license.KeyStoreParam;
import de.schlichtherle.license.LicenseContent;
import de.schlichtherle.license.LicenseParam;

public class LicenseVerifier {

    private static final Logger LOG = Log.logger(LicenseVerifier.class);

    private static final String LICENSE_PARAM_PATH = "/verify-license.json";

    private static volatile LicenseVerifier INSTANCE = null;

    private final LicenseVerifyParam verifyParam;
    private final LicenseVerifyManager manager;

    private LicenseVerifier(HugeConfig config) {
        E.checkNotNull(config, "huge config");
        this.verifyParam = buildVerifyParam(LICENSE_PARAM_PATH);
        LicenseParam licenseParam = this.initLicenseParam(this.verifyParam);
        this.manager = new LicenseVerifyManager(licenseParam, config);
    }

    public static LicenseVerifier instance(HugeConfig config) {
        if (INSTANCE == null) {
            synchronized(LicenseVerifier.class) {
                if (INSTANCE == null) {
                    INSTANCE = new LicenseVerifier(config);
                }
            }
        }
        return INSTANCE;
    }

    public synchronized void install() {
        try {
            this.manager.uninstall();
            File licenseFile = new File(this.verifyParam.getLicensePath());
            LicenseContent content = this.manager.install(licenseFile);
            LOG.info("The license is successfully installed, valid for {} - {}",
                     content.getNotBefore(), content.getNotAfter());
        } catch (Exception e) {
            throw new HugeException("Failed to install license", e);
        }
    }

    public void verify() {
        try {
            LicenseContent content = this.manager.verify();
            LOG.info("The license verification passed, valid for {} - {}",
                     content.getNotBefore(), content.getNotAfter());
        } catch (Exception e) {
            throw new HugeException("The license verification failed!", e);
        }
    }

    private LicenseParam initLicenseParam(LicenseVerifyParam param) {
        Preferences preferences = Preferences.userNodeForPackage(
                                  LicenseVerifier.class);
        CipherParam cipherParam = new DefaultCipherParam(
                                  param.getStorePassword());
        KeyStoreParam keyStoreParam = new DefaultKeyStoreParam(
                                      LicenseVerifier.class,
                                      param.getPublicKeyPath(),
                                      param.getPublicAlias(),
                                      param.getStorePassword(),
                                      null);
        return new DefaultLicenseParam(param.getSubject(), preferences,
                                       keyStoreParam, cipherParam);
    }

    private static LicenseVerifyParam buildVerifyParam(String path) {
        InputStream stream = LicenseVerifier.class.getResourceAsStream(path);
        ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.readValue(stream, LicenseVerifyParam.class);
        } catch (IOException e) {
            throw new RuntimeException(String.format(
                      "Failed to read file '%s'", path));
        }
    }
}

