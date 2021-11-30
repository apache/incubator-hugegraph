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

package com.baidu.hugegraph.unit.license;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.prefs.Preferences;

import javax.security.auth.x500.X500Principal;

import org.apache.commons.codec.Charsets;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Test;

import com.baidu.hugegraph.license.CommonLicenseManager;
import com.baidu.hugegraph.license.ExtraParam;
import com.baidu.hugegraph.license.FileKeyStoreParam;
import com.baidu.hugegraph.license.LicenseCreateParam;
import com.baidu.hugegraph.license.LicenseVerifyParam;
import com.baidu.hugegraph.testutil.Assert;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import de.schlichtherle.license.CipherParam;
import de.schlichtherle.license.DefaultCipherParam;
import de.schlichtherle.license.DefaultLicenseParam;
import de.schlichtherle.license.KeyStoreParam;
import de.schlichtherle.license.LicenseContent;
import de.schlichtherle.license.LicenseContentException;
import de.schlichtherle.license.LicenseManager;
import de.schlichtherle.license.LicenseParam;
import de.schlichtherle.license.NoLicenseInstalledException;

public class LicenseManagerTest {

    private static final Charset CHARSET = Charsets.UTF_8;
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @After
    public void teardown() throws IOException {
        File lic = new File("src/test/resources/hugegraph-evaluation.license");
        FileUtils.forceDelete(lic);
    }

    @Test
    public void testCreateInstallVerifyLicense() throws IOException {
        String createConfigPath = "src/test/resources/create-license.json";
        LicenseCreator creator = LicenseCreator.build(createConfigPath);
        creator.create();

        String verifyConfigPath = "src/test/resources/verify-license.json";
        LicenseVerifier verifier = LicenseVerifier.build(verifyConfigPath);
        verifier.install();
        verifier.verify();
    }

    @Test
    public void testCreateVerifyLicenseWithoutInstall() throws IOException {
        String createConfigPath = "src/test/resources/create-license.json";
        LicenseCreator creator = LicenseCreator.build(createConfigPath);
        creator.create();

        String verifyConfigPath = "src/test/resources/verify-license.json";
        LicenseVerifier verifier = LicenseVerifier.build(verifyConfigPath);
        verifier.uninstall();
        Assert.assertThrows(RuntimeException.class, () -> {
            verifier.verify();
        }, e -> {
            Assert.assertEquals(NoLicenseInstalledException.class,
                                e.getCause().getClass());
        });
    }

    private static class LicenseCreator {

        private static final X500Principal DEFAULT_ISSUER = new X500Principal(
                "CN=localhost, OU=localhost, O=localhost, L=SH, ST=SH, C=CN");

        private final LicenseCreateParam param;

        public LicenseCreator(LicenseCreateParam param) {
            this.param = param;
        }

        public static LicenseCreator build(String path) throws IOException {
            File file = FileUtils.getFile(path);
            String json;
            try {
                json = FileUtils.readFileToString(file, CHARSET);
            } catch (IOException e) {
                throw new RuntimeException(String.format(
                          "Failed to read file '%s'", path));
            }
            LicenseCreateParam param = MAPPER.readValue(
                                       json, LicenseCreateParam.class);
            return new LicenseCreator(param);
        }

        public void create(){
            try {
                LicenseParam licenseParam = this.initLicenseParam();
                LicenseManager manager = new LicenseCreateManager(licenseParam);
                LicenseContent licenseContent = this.initLicenseContent();
                File licenseFile = new File(this.param.licensePath());
                manager.store(licenseContent, licenseFile);
            } catch (Exception e){
                throw new RuntimeException("Generate license failed", e);
            }
        }

        private LicenseParam initLicenseParam(){
            Preferences preferences = Preferences.userNodeForPackage(
                                      LicenseCreator.class);
            CipherParam cipherParam = new DefaultCipherParam(
                                      this.param.storePassword());
            KeyStoreParam keyStoreParam;
            keyStoreParam = new FileKeyStoreParam(LicenseCreator.class,
                                                  this.param.privateKeyPath(),
                                                  this.param.privateAlias(),
                                                  this.param.storePassword(),
                                                  this.param.keyPassword());
            return new DefaultLicenseParam(this.param.subject(), preferences,
                                           keyStoreParam, cipherParam);
        }

        private LicenseContent initLicenseContent(){
            LicenseContent content = new LicenseContent();
            content.setHolder(DEFAULT_ISSUER);
            content.setIssuer(DEFAULT_ISSUER);
            content.setSubject(this.param.subject());
            content.setIssued(this.param.issuedTime());
            content.setNotBefore(this.param.notBefore());
            content.setNotAfter(this.param.notAfter());
            content.setConsumerType(this.param.consumerType());
            content.setConsumerAmount(this.param.consumerAmount());
            content.setInfo(this.param.description());
            // Customized verification params
            String json;
            try {
                json = MAPPER.writeValueAsString(this.param.extraParams());
            } catch (JsonProcessingException e) {
                throw new RuntimeException("Failed to write extra params", e);
            }
            content.setExtra(json);
            return content;
        }
    }

    private static class LicenseCreateManager extends CommonLicenseManager {

        public LicenseCreateManager(LicenseParam param) {
            super(param);
        }

        @Override
        protected synchronized void validateCreate(LicenseContent content)
                  throws LicenseContentException {
            super.validateCreate(content);
        }
    }

    private static class LicenseVerifier {

        private final LicenseVerifyParam param;
        private final LicenseVerifyManager manager;

        public LicenseVerifier(LicenseVerifyParam param) {
            this.param = param;
            LicenseParam licenseParam = this.initLicenseParam(param);
            this.manager = new LicenseVerifyManager(licenseParam);
        }

        public static LicenseVerifier build(String path) throws IOException {
            File file = FileUtils.getFile(path);
            String json;
            try {
                json = FileUtils.readFileToString(file, CHARSET);
            } catch (IOException e) {
                throw new RuntimeException(String.format(
                          "Failed to read file '%s'", path));
            }
            LicenseVerifyParam param = MAPPER.readValue(
                                       json, LicenseVerifyParam.class);
            return new LicenseVerifier(param);
        }

        public synchronized void install() {
            try {
                this.manager.uninstall();
                File licenseFile = new File(this.param.licensePath());
                this.manager.install(licenseFile);
            } catch (Exception e) {
                throw new RuntimeException("Failed to install license", e);
            }
        }

        public synchronized void uninstall() {
            try {
                this.manager.uninstall();
            } catch (Exception e) {
                throw new RuntimeException("Failed to uninstall license", e);
            }
        }

        public void verify() {
            try {
                this.manager.verify();
            } catch (Exception e) {
                throw new RuntimeException("The license verify failed", e);
            }
        }

        private LicenseParam initLicenseParam(LicenseVerifyParam param) {
            Preferences preferences = Preferences.userNodeForPackage(
                                      LicenseVerifier.class);
            CipherParam cipherParam = new DefaultCipherParam(
                                      param.storePassword());
            KeyStoreParam keyStoreParam = new FileKeyStoreParam(
                                          LicenseVerifier.class,
                                          param.publicKeyPath(),
                                          param.publicAlias(),
                                          param.storePassword(),
                                          null);
            return new DefaultLicenseParam(param.subject(), preferences,
                                           keyStoreParam, cipherParam);
        }
    }

    private static class LicenseVerifyManager extends CommonLicenseManager {

        private final String serverId = "server-1";

        public LicenseVerifyManager(LicenseParam param) {
            super(param);
        }

        @Override
        protected synchronized void validate(LicenseContent content)
                  throws LicenseContentException {
            // Call super validate firstly, will verify expired
            super.validate(content);

            // Verify the customized license parameters
            String extra = (String) content.getExtra();
            List<ExtraParam> extraParams;
            try {
                TypeReference<List<ExtraParam>> type;
                type = new TypeReference<List<ExtraParam>>() {};
                extraParams = MAPPER.readValue(extra, type);
            } catch (IOException e) {
                throw new RuntimeException("Failed to read extra params", e);
            }
            ExtraParam param = this.matchParam(this.serverId, extraParams);
            if (param == null) {
                throw newLicenseException("The current server's id '%s' " +
                                          "is not authorized");
            }
            if (this.usingGraphs() > param.graphs()) {
                throw newLicenseException("The using graphs exceeded '%s'" +
                                          "authorized limit '%s'",
                                          this.usingGraphs(), param.graphs());
            }
        }

        private int usingGraphs() {
            // Assume
            return 2;
        }

        private ExtraParam matchParam(String id, List<ExtraParam> extraParams) {
            for (ExtraParam param : extraParams) {
                if (param.id().equals(id)) {
                    return param;
                }
            }
            return null;
        }

        private LicenseContentException newLicenseException(String message,
                                                            Object... args) {
            return new LicenseContentException(String.format(message, args));
        }
    }
}
