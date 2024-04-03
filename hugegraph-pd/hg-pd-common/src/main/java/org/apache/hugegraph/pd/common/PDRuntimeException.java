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

package org.apache.hugegraph.pd.common;

public class PDRuntimeException extends RuntimeException {

    // public static final int LICENSE_ERROR = -11;

    private int errorCode = 0;

    public PDRuntimeException(int error) {
        super(String.format("Error code = %d", error));
        this.errorCode = error;
    }

    public PDRuntimeException(int error, String msg) {
        super(msg);
        this.errorCode = error;
    }

    public PDRuntimeException(int error, Throwable e) {
        super(e);
        this.errorCode = error;
    }

    public PDRuntimeException(int error, String msg, Throwable e) {
        super(msg, e);
        this.errorCode = error;
    }

    public int getErrorCode() {
        return errorCode;
    }
}
