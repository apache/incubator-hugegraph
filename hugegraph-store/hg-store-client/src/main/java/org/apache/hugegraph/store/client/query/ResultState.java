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

package org.apache.hugegraph.store.client.query;

/**
 *                                                        |---(has more result) --> IDLE
 *                                                        |
 *  IDLE --(send req)--> WAITING --(onNext)--> INNER_BUSY |---(onCompleted)--> FINISHED
 *                                                        |
 *                                                        |---(error)----> ERROR
 *                                                        |
 *                                                        |---(processing)-> BUSY (ERROR)
 */
public enum ResultState {
    // 初始化的状态，可以读取新数据
    IDLE,
    // 发送数据到等待server返回中间的状态
    WAITING,
    // 开始读取数据,后的状态
    INNER_BUSY,
    // 没更多数据了
    FINISHED,
    // 写入数据的状态
    // 错误
    ERROR;

    private String message;

    public String getMessage() {
        return message;
    }

    public ResultState setMessage(String message) {
        this.message = message;
        return this;
    }
}
