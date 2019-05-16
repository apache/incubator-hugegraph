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

package com.baidu.hugegraph.backend.store.postgresql;

import com.baidu.hugegraph.backend.store.mysql.MysqlSessions;
import com.baidu.hugegraph.config.HugeConfig;

public class PostgresqlSessions extends MysqlSessions {

    public PostgresqlSessions(HugeConfig config, String database, String store) {
        super(config, database, store);
    }

    @Override
    protected String buildCreateDatabase(String database) {
        return String.format("CREATE DATABASE %s ENCODING='UTF-8'", database);
    }
}
