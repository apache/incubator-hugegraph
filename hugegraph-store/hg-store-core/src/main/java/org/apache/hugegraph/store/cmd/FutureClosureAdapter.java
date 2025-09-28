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

package org.apache.hugegraph.store.cmd;

import java.util.concurrent.CompletableFuture;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Status;

@Deprecated
public class FutureClosureAdapter<T> implements Closure {

    public final CompletableFuture<T> future = new CompletableFuture<>();
    private T resp;

    public T getResponse() {
        return this.resp;
    }

    public void setResponse(T resp) {
        this.resp = resp;
        future.complete(resp);
        run(Status.OK());
    }

    public void failure(Throwable t) {
        future.completeExceptionally(t);
        run(new Status(-1, t.getMessage()));
    }

    @Override
    public void run(Status status) {

    }
}
