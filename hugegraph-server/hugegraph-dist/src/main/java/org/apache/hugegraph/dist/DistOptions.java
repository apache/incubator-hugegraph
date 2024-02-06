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

package org.apache.hugegraph.dist;

import static org.apache.hugegraph.config.OptionChecker.disallowEmpty;

import org.apache.hugegraph.config.ConfigListOption;
import org.apache.hugegraph.config.OptionHolder;

public class DistOptions extends OptionHolder {

    private DistOptions() {
        super();
    }

    private static volatile DistOptions instance;

    public static synchronized DistOptions instance() {
        if (instance == null) {
            instance = new DistOptions();
            instance.registerOptions();
        }
        return instance;
    }

    public static final ConfigListOption<String> BACKENDS =
            new ConfigListOption<>(
                    "backends",
                    "The all data store type.",
                    disallowEmpty(),
                    "memory"
            );
}
