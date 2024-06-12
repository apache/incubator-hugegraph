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

package org.apache.hugegraph.rocksdb.access;

import java.io.InterruptedIOException;

public class DBStoreException extends RuntimeException {

    private static final long serialVersionUID = 5956983547131986887L;

    public DBStoreException(String message) {
        super(message);
    }

    public DBStoreException(String message, Throwable cause) {
        super(message, cause);
    }

    public DBStoreException(String message, Object... args) {
        super(String.format(message, args));
    }

    public DBStoreException(String message, Throwable cause, Object... args) {
        super(String.format(message, args), cause);
    }

    public DBStoreException(Throwable cause) {
        this("Exception in DBStore " + cause.getMessage(), cause);
    }

    public static Throwable rootCause(Throwable e) {
        Throwable cause = e;
        while (cause.getCause() != null) {
            cause = cause.getCause();
        }
        return cause;
    }

    public static boolean isInterrupted(Throwable e) {
        Throwable rootCause = DBStoreException.rootCause(e);
        return rootCause instanceof InterruptedException ||
               rootCause instanceof InterruptedIOException;
    }

    public Throwable rootCause() {
        return rootCause(this);
    }
}
