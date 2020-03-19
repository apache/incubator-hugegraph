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
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import de.schlichtherle.license.AbstractKeyStoreParam;

/**
 * Custom KeyStoreParam to store public and private key storage files to
 * other disk locations instead of projects
 */
public class FileKeyStoreParam extends AbstractKeyStoreParam {

    private String storePath;
    private String alias;
    private String keyPwd;
    private String storePwd;

    public FileKeyStoreParam(Class<?> clazz, String resource, String alias,
                             String storePwd, String keyPwd) {
        super(clazz, resource);
        this.storePath = resource;
        this.alias = alias;
        this.storePwd = storePwd;
        this.keyPwd = keyPwd;
    }

    @Override
    public String getAlias() {
        return this.alias;
    }

    @Override
    public String getStorePwd() {
        return this.storePwd;
    }

    @Override
    public String getKeyPwd() {
        return this.keyPwd;
    }

    @Override
    public InputStream getStream() throws IOException {
        return new FileInputStream(new File(this.storePath));
    }
}
