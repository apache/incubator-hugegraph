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

import java.util.Date;
import java.util.List;

import org.apache.commons.lang3.time.DateUtils;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;

public class LicenseCreateParam {

    @JsonProperty("subject")
    private String subject;

    @JsonProperty("private_alias")
    private String privateAlias;

    @JsonProperty("key_password")
    private String keyPassword;

    @JsonProperty("store_password")
    private String storePassword;

    @JsonProperty("privatekey_path")
    private String privateKeyPath;

    @JsonProperty("license_path")
    private String licensePath;

    @JsonProperty("issued_time")
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss", timezone = "GMT+8")
    private Date issuedTime = new Date();

    @JsonProperty("not_before")
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss", timezone = "GMT+8")
    private Date notBefore = this.issuedTime;

    @JsonProperty("not_after")
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss", timezone = "GMT+8")
    private Date notAfter = DateUtils.addDays(this.notBefore, 30);

    @JsonProperty("consumer_type")
    private String consumerType = "user";

    @JsonProperty("consumer_amount")
    private Integer consumerAmount = 1;

    @JsonProperty("description")
    private String description = "";

    @JsonProperty("extra_params")
    private List<ExtraParam> extraParams;

    public String subject() {
        return this.subject;
    }

    public String privateAlias() {
        return this.privateAlias;
    }

    public String keyPassword() {
        return this.keyPassword;
    }

    public String storePassword() {
        return this.storePassword;
    }

    public String privateKeyPath() {
        return this.privateKeyPath;
    }

    public String licensePath() {
        return this.licensePath;
    }

    public Date issuedTime() {
        return this.issuedTime;
    }

    public Date notBefore() {
        return this.notBefore;
    }

    public Date notAfter() {
        return this.notAfter;
    }

    public String consumerType() {
        return this.consumerType;
    }

    public Integer consumerAmount() {
        return this.consumerAmount;
    }

    public String description() {
        return this.description;
    }

    public List<ExtraParam> extraParams() {
        return this.extraParams;
    }
}
