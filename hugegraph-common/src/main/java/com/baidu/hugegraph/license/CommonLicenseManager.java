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

import java.beans.XMLDecoder;
import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;

import org.slf4j.Logger;

import com.baidu.hugegraph.util.Log;

import de.schlichtherle.license.LicenseContent;
import de.schlichtherle.license.LicenseContentException;
import de.schlichtherle.license.LicenseManager;
import de.schlichtherle.license.LicenseNotary;
import de.schlichtherle.license.LicenseParam;
import de.schlichtherle.license.NoLicenseInstalledException;
import de.schlichtherle.xml.GenericCertificate;

public class CommonLicenseManager extends LicenseManager {

    private static final Logger LOG = Log.logger(CommonLicenseManager.class);

    private static final String CHARSET = "UTF-8";
    private static final int BUF_SIZE = 8 * 1024;

    public CommonLicenseManager(LicenseParam param) {
        super(param);
    }

    @Override
    protected synchronized byte[] create(LicenseContent content,
                                         LicenseNotary notary)
                                         throws Exception {
        super.initialize(content);
        this.validateCreate(content);
        GenericCertificate certificate = notary.sign(content);
        return super.getPrivacyGuard().cert2key(certificate);
    }

    @Override
    protected synchronized LicenseContent install(byte[] key,
                                                  LicenseNotary notary)
                                                  throws Exception {
        GenericCertificate certificate = super.getPrivacyGuard().key2cert(key);
        notary.verify(certificate);
        String encodedText = certificate.getEncoded();
        LicenseContent content = (LicenseContent) this.load(encodedText);
        this.validate(content);
        super.setLicenseKey(key);
        super.setCertificate(certificate);
        return content;
    }

    @Override
    protected synchronized LicenseContent verify(LicenseNotary notary)
                                                 throws Exception {
        // Load license key from preferences
        byte[] key = super.getLicenseKey();
        if (key == null) {
            String subject = super.getLicenseParam().getSubject();
            throw new NoLicenseInstalledException(subject);
        }

        GenericCertificate certificate = super.getPrivacyGuard().key2cert(key);
        notary.verify(certificate);
        String encodedText = certificate.getEncoded();
        LicenseContent content = (LicenseContent) this.load(encodedText);
        this.validate(content);
        super.setCertificate(certificate);
        return content;
    }

    @Override
    protected synchronized void validate(LicenseContent content)
                                         throws LicenseContentException {
        // Call super validate, expected to be overwritten
        super.validate(content);
    }

    protected synchronized void validateCreate(LicenseContent content)
                                               throws LicenseContentException {
        // Just call super validate is ok
        super.validate(content);
    }

    private Object load(String text) throws Exception {
        InputStream bis = null;
        XMLDecoder decoder = null;
        try {
            bis = new ByteArrayInputStream(text.getBytes(CHARSET));
            decoder = new XMLDecoder(new BufferedInputStream(bis, BUF_SIZE));
            return decoder.readObject();
        } catch (UnsupportedEncodingException e) {
            throw new LicenseContentException(String.format(
                      "Unsupported charset: %s", CHARSET));
        } finally {
            if (decoder != null) {
                decoder.close();
            }
            try {
                if (bis != null) {
                    bis.close();
                }
            } catch (Exception e) {
                LOG.warn("Failed to close stream", e);
            }
        }
    }
}
