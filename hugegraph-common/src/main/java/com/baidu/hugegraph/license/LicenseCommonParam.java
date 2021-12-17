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

public class LicenseCommonParam {

    @JsonProperty("subject")
    private String subject;

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
    private List<LicenseExtraParam> extraParams;

    public LicenseCommonParam() {
        // pass
    }

    public LicenseCommonParam(String subject, String description,
                              Date issued, Date notBefore, Date notAfter,
                              String consumerType, int consumerAmount,
                              List<LicenseExtraParam> extraParams) {
        this.subject = subject;
        this.description = description;
        this.issuedTime = issued;
        this.notBefore = notBefore;
        this.notAfter = notAfter;
        this.consumerType = consumerType;
        this.consumerAmount = consumerAmount;
        this.extraParams = extraParams;
    }

    public String subject() {
        return this.subject;
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

    public List<LicenseExtraParam> extraParams() {
        return this.extraParams;
    }
}
