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

public class LicenseVerifyParam {

    private String subject;

    private String publicAlias;

    private String storePassword;

    private String licensePath;

    private String publicKeyPath;

    public LicenseVerifyParam() {
    }

    public LicenseVerifyParam(String subject, String publicAlias,
                              String storePassword, String licensePath,
                              String publicKeyPath) {
        this.subject = subject;
        this.publicAlias = publicAlias;
        this.storePassword = storePassword;
        this.licensePath = licensePath;
        this.publicKeyPath = publicKeyPath;
    }

    public String getSubject() {
        return this.subject;
    }

    public void setSubject(String subject) {
        this.subject = subject;
    }

    public String getPublicAlias() {
        return this.publicAlias;
    }

    public void setPublicAlias(String publicAlias) {
        this.publicAlias = publicAlias;
    }

    public String getStorePassword() {
        return this.storePassword;
    }

    public void setStorePassword(String storePassword) {
        this.storePassword = storePassword;
    }

    public String getLicensePath() {
        return this.licensePath;
    }

    public void setLicensePath(String licensePath) {
        this.licensePath = licensePath;
    }

    public String getPublicKeyPath() {
        return this.publicKeyPath;
    }

    public void setPublicKeyPath(String publicKeyPath) {
        this.publicKeyPath = publicKeyPath;
    }
}

