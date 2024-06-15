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

package org.apache.hugegraph.store.node.model;

import java.util.Objects;

/**
 * created on 2021/11/1
 */
public class HgNodeStatus {

    private int status;
    private String text;

    public HgNodeStatus(int status, String text) {
        this.status = status;
        this.text = text;
    }

    public int getStatus() {
        return status;
    }

    public HgNodeStatus setStatus(int status) {
        this.status = status;
        return this;
    }

    public String getText() {
        return text;
    }

    public HgNodeStatus setText(String text) {
        this.text = text;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        HgNodeStatus that = (HgNodeStatus) o;
        return status == that.status && Objects.equals(text, that.text);
    }

    @Override
    public int hashCode() {
        return Objects.hash(status, text);
    }

    @Override
    public String toString() {
        return "HgNodeStatus{" +
               "status=" + status +
               ", text='" + text + '\'' +
               '}';
    }
}
