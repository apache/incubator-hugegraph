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
import java.util.Date;
import java.util.List;

import org.slf4j.Logger;

import com.baidu.hugegraph.util.JsonUtil;
import com.baidu.hugegraph.util.Log;

import de.schlichtherle.license.LicenseContent;
import de.schlichtherle.license.LicenseContentException;
import de.schlichtherle.license.LicenseManager;
import de.schlichtherle.license.LicenseNotary;
import de.schlichtherle.license.LicenseParam;
import de.schlichtherle.license.NoLicenseInstalledException;
import de.schlichtherle.xml.GenericCertificate;

public class CustomLicenseManager extends LicenseManager {

    private static final Logger LOG = Log.logger(CustomLicenseManager.class);

    private static final String CHARSET = "UTF-8";
    private static final int BUF_SIZE = 8 * 1024;

    public CustomLicenseManager(LicenseParam param) {
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
            throw new NoLicenseInstalledException(super.getLicenseParam()
                                                       .getSubject());
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
        // call super validate firstly
        super.validate(content);

        // Verify the customized license parameters.
        String extraInfo = (String) content.getExtra();
        if (extraInfo == null) {
            return;
        }
        CustomCheckParam param = JsonUtil.fromJson(extraInfo,
                                                   CustomCheckParam.class);
        // Current server information
        MachineInfo machineInfo = new MachineInfo();
        if(!checkIpOrMac(param.getIpList(), machineInfo.getIpAddress())) {
            throw new LicenseContentException("The current server's ip " +
                                              "is not authorized");
        }
        if (!checkIpOrMac(param.getMacList(), machineInfo.getMacAddress())) {
            throw new LicenseContentException("The current server's mac " +
                                              "is not authorized");
        }
    }

    private synchronized void validateCreate(LicenseContent content)
            throws LicenseContentException {
        Date now = new Date();
        Date notBefore = content.getNotBefore();
        Date notAfter = content.getNotAfter();
        if (notAfter != null && now.after(notAfter)) {
            throw new LicenseContentException("License expiration time cannot " +
                                              "be earlier than current time");
        }
        if (notBefore != null && notAfter != null &&
            notAfter.before(notBefore)) {
            throw new LicenseContentException("License issuance time cannot " +
                                              "be later than expiration time");
        }
        String consumerType = content.getConsumerType();
        if (consumerType == null) {
            throw new LicenseContentException("Consumer type cannot be empty");
        }
    }

    private Object load(String encoded) throws Exception {
        BufferedInputStream stream = null;
        XMLDecoder decoder = null;
        try {
            InputStream bis = new ByteArrayInputStream(encoded.getBytes(CHARSET));
            stream = new BufferedInputStream(bis);
            decoder = new XMLDecoder(new BufferedInputStream(stream, BUF_SIZE));
            return decoder.readObject();
        } catch (UnsupportedEncodingException e) {
            throw new LicenseContentException(String.format(
                      "Unsupported charset: %s", CHARSET));
        } finally {
            try {
                if (decoder != null) {
                    decoder.close();
                }
                if (stream != null) {
                    stream.close();
                }
            } catch (Exception e) {
                LOG.warn("Failed to close decoder or stream", e);
            }
        }
    }

    private boolean checkIpOrMac(List<String> expectList,
                                 List<String> actualList) {
        if (expectList == null || expectList.isEmpty()) {
            return true;
        }
        if (actualList != null && actualList.size() > 0) {
            for (String expect : expectList ){
                if (actualList.contains(expect.trim())) {
                    return true;
                }
            }
        }
        return false;
    }
}
