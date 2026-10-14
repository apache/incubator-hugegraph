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

package org.apache.hugegraph.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;

public final class CloseFailure {

    private static final Logger LOG = Log.logger(CloseFailure.class);

    private CloseFailure() {
    }

    public static Throwable collect(Throwable first, Throwable next) {
        if (first == null || first == next) {
            return first == null ? next : first;
        }
        Throwable primary = next instanceof Error && !(first instanceof Error) ? next : first;
        Throwable secondary = primary == first ? next : first;
        if (!referencesFailure(primary, secondary)) {
            if (referencesFailure(secondary, primary)) {
                // Keep the original diagnostic without introducing a cause/suppression cycle.
                LOG.error("Additional cleanup failure cannot be attached without an exception cycle", secondary);
            } else {
                primary.addSuppressed(secondary);
            }
        }
        return primary;
    }

    private static boolean referencesFailure(Throwable root, Throwable target) {
        Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
        List<Throwable> pending = new ArrayList<>();
        pending.add(root);
        while (!pending.isEmpty()) {
            Throwable current = pending.remove(pending.size() - 1);
            if (current == target) {
                return true;
            }
            if (!seen.add(current)) {
                continue;
            }
            if (current.getCause() != null) {
                pending.add(current.getCause());
            }
            pending.addAll(Arrays.asList(current.getSuppressed()));
        }
        return false;
    }

}
