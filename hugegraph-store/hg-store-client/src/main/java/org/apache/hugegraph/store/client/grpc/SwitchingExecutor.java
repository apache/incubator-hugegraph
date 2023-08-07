/*
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

package org.apache.hugegraph.store.client.grpc;

import java.util.Optional;
import java.util.function.Supplier;

import javax.annotation.concurrent.ThreadSafe;

/**
 * 2021/12/1
 */
@ThreadSafe
final class SwitchingExecutor {

    private SwitchingExecutor() {
    }

    static SwitchingExecutor of() {
        return new SwitchingExecutor();
    }

    <T> Optional<T> invoke(Supplier<Boolean> switcher, Supplier<T> trueSupplier,
                           Supplier<T> falseSupplier) {
        Optional<T> option = null;

        if (switcher.get()) {
            option = Optional.of(trueSupplier.get());
        } else {
            option = Optional.of(falseSupplier.get());
        }
        if (option == null) {
            option = Optional.empty();
        }

        return option;
    }
}
