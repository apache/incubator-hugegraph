/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hugegraph.store.cli;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import lombok.Data;

@Data
@Component
public class AppConfig {

    @Value("${pd.address}")
    private String pdAddress;

    @Value("${net.kv.scanner.page.size}")
    private int scannerPageSize;

    @Value("${scanner.graph}")
    private String scannerGraph;

    @Value("${scanner.table}")
    private String scannerTable;

    @Value("${scanner.max}")
    private int scannerMax;

    @Value("${scanner.mod}")
    private int scannerModNumber;

    @Value("${committer.graph}")
    private String committerGraph;

    @Value("${committer.table}")
    private String committerTable;

    @Value("${committer.amount}")
    private int committerAmount;
}
