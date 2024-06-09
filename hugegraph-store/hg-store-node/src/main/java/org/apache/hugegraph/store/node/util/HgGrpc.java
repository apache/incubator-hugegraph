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

package org.apache.hugegraph.store.node.util;

import org.apache.hugegraph.store.grpc.common.Key;
import org.apache.hugegraph.store.grpc.common.Kv;
import org.apache.hugegraph.store.grpc.common.ResCode;
import org.apache.hugegraph.store.grpc.common.ResStatus;
import org.apache.hugegraph.store.grpc.common.Tk;
import org.apache.hugegraph.store.grpc.common.Tse;
import org.apache.hugegraph.store.term.HgPair;

import com.google.common.base.Throwables;
import com.google.protobuf.ByteString;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import jline.internal.Log;

public abstract class HgGrpc {

    private static final ResCode OK = ResCode.RES_CODE_OK;

    public static ResStatus not() {
        return toStatus(ResCode.RES_CODE_NOT_EXIST, "not exist");
    }

    public static ResStatus fail() {
        return toStatus(ResCode.RES_CODE_FAIL, "failure");
    }

    public static ResStatus fail(String msg) {
        return toStatus(ResCode.RES_CODE_FAIL, msg);
    }

    public static ResStatus success() {
        return success("success");
    }

    public static ResStatus success(String msg) {
        return toStatus(ResCode.RES_CODE_OK, msg);
    }

    public static ResStatus toStatus(ResCode code, String msg) {
        return ResStatus.newBuilder()
                        .setCode(code)
                        .setMsg(msg).build();
    }

    public static <K, V> HgPair<K, V> toHgPair(Key key) {
        return new HgPair(key.getCode(), key.getKey().toByteArray());
    }

    public static <K, V> HgPair<K, V> toHgPair(Tk tk) {
        return new HgPair(
                tk.getTable(), tk.getKey().toByteArray()
        );
    }

    public static Kv toKv(HgPair<byte[], byte[]> pair) {
        return toKv(pair, Kv.newBuilder());
    }

    public static Kv toKv(HgPair<byte[], byte[]> pair, Kv.Builder builder) {
        return builder.clear()
                      .setKey(ByteString.copyFrom(pair.getKey()))
                      .setValue(ByteString.copyFrom(pair.getValue()))
                      .build();
    }

    public static <K, V> HgPair<K, V> toHgPair(Tse tse) {
        return new HgPair(
                tse.getStart().getKey().toByteArray(),
                tse.getEnd().getKey().toByteArray()
        );
    }

    public static StatusRuntimeException toErr(String msg) {
        return toErr(Status.INTERNAL, msg, null);
    }

    public static StatusRuntimeException toErr(Status.Code code,
                                               String des) {
        return toErr(code, des, null);
    }

    public static StatusRuntimeException toErr(Status.Code code,
                                               String des,
                                               Throwable t) {
        return toErr(code.toStatus(), des, t);
    }

    public static StatusRuntimeException toErr(Status status,
                                               String des,
                                               Throwable t) {
        if (t != null) {
            // 为给client返回完整异常信息
            des = (des == null ? "" : des + ",") +
                  Throwables.getStackTraceAsString(t);
        }
        Status wdStatus = status.withDescription(des);
        Log.error(wdStatus);
        Status fullStatus = wdStatus.withCause(t);
        return new StatusRuntimeException(fullStatus);
    }

}
