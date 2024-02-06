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

package org.apache.hugegraph.backend.id;

import org.apache.hugegraph.util.E;

public interface Id extends Comparable<Id> {

    int UUID_LENGTH = 16;

    Object asObject();

    String asString();

    long asLong();

    byte[] asBytes();

    int length();

    IdType type();

    default boolean number() {
        return this.type() == IdType.LONG;
    }

    default boolean uuid() {
        return this.type() == IdType.UUID;
    }

    default boolean string() {
        return this.type() == IdType.STRING;
    }

    default boolean edge() {
        return this.type() == IdType.EDGE;
    }

    enum IdType {

        UNKNOWN,
        LONG,
        UUID,
        STRING,
        EDGE;

        public char prefix() {
            if (this == UNKNOWN) {
                return 'N';
            }
            return this.name().charAt(0);
        }

        public static IdType valueOfPrefix(String id) {
            E.checkArgument(id != null && id.length() > 0,
                            "Invalid id '%s'", id);
            switch (id.charAt(0)) {
                case 'L':
                    return IdType.LONG;
                case 'U':
                    return IdType.UUID;
                case 'S':
                    return IdType.STRING;
                case 'E':
                    return IdType.EDGE;
                default:
                    return IdType.UNKNOWN;
            }
        }
    }
}
