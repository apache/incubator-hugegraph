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

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonProperty;

public class LicenseCreateParam extends LicenseCommonParam {

    @JsonProperty("private_alias")
    private String privateAlias;

    @JsonAlias("key_ticket")
    @JsonProperty("key_password")
    private String keyPassword;

    @JsonAlias("store_ticket")
    @JsonProperty("store_password")
    private String storePassword;

    @JsonProperty("privatekey_path")
    private String privateKeyPath;

    @JsonProperty("license_path")
    private String licensePath;

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
}
