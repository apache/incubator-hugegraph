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
import java.util.Properties;
import java.util.prefs.Preferences;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.util.Log;

import de.schlichtherle.license.CipherParam;
import de.schlichtherle.license.DefaultCipherParam;
import de.schlichtherle.license.DefaultLicenseParam;
import de.schlichtherle.license.KeyStoreParam;
import de.schlichtherle.license.LicenseContent;
import de.schlichtherle.license.LicenseManager;
import de.schlichtherle.license.LicenseParam;

public class LicenseVerifier {

    private static final Logger LOG = Log.logger(LicenseVerifier.class);

    private static final String LICENSE_PARAMS = "/verify-license.properties";

    private static final LicenseVerifier INSTANCE = new LicenseVerifier();

    private LicenseVerifier() {
    }

    public static LicenseVerifier instance() {
        return INSTANCE;
    }

    public synchronized void install() {
        InputStream stream = null;
        LicenseVerifyParam param;
        try {
            stream = getClass().getResourceAsStream(LICENSE_PARAMS);
            Properties properties = new Properties();
            properties.load(stream);
            param = buildVerifyParam(properties);
        } catch (IOException e) {
            throw new HugeException("Failed to install license", e);
        } finally {
            if (stream != null) {
                IOUtils.closeQuietly(stream);
            }
        }

        try {
            LicenseParam licenseParam = this.initLicenseParam(param);
            LicenseManager licenseManager = LicenseManagerHolder.instance(
                                                                 licenseParam);
            licenseManager.uninstall();
            File licenseFile = new File(param.getLicensePath());
            LicenseContent content = licenseManager.install(licenseFile);
            LOG.info("The license is successfully installed, valid for {} - {}",
                     content.getNotBefore(), content.getNotAfter());
        } catch (Exception e) {
            throw new HugeException("Failed to install license", e);
        }
    }

    public void verify() {
        LicenseManager licenseManager = LicenseManagerHolder.instance(null);
        try {
            LicenseContent content = licenseManager.verify();
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
        KeyStoreParam keyStoreParam = new CustomKeyStoreParam(
                                          LicenseVerifier.class,
                                          param.getPublicKeyPath(),
                                          param.getPublicAlias(),
                                          param.getStorePassword(),
                                          null);
        return new DefaultLicenseParam(param.getSubject(), preferences,
                                       keyStoreParam, cipherParam);
    }

    private static LicenseVerifyParam buildVerifyParam(Properties properties) {
        LicenseVerifyParam param = new LicenseVerifyParam();
        param.setPublicAlias(properties.getProperty("public_alias"));
        param.setSubject(properties.getProperty("subject"));
        param.setStorePassword(properties.getProperty("store_password"));
        param.setPublicKeyPath(properties.getProperty("publickey_path"));
        param.setLicensePath(properties.getProperty("license_path"));
        return param;
    }
}

