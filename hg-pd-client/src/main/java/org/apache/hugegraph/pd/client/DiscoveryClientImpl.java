<<<<<<<< HEAD:hugegraph-pd/hg-pd-client/src/main/java/org/apache/hugegraph/pd/client/DiscoveryClientImpl.java
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

========
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-client/src/main/java/org/apache/hugegraph/pd/client/DiscoveryClientImpl.java
package org.apache.hugegraph.pd.client;

import java.util.Map;
import java.util.function.Consumer;

<<<<<<<< HEAD:hugegraph-pd/hg-pd-client/src/main/java/org/apache/hugegraph/pd/client/DiscoveryClientImpl.java
import org.apache.hugegraph.pd.common.Useless;
import org.apache.hugegraph.pd.grpc.discovery.NodeInfo;
import org.apache.hugegraph.pd.grpc.discovery.RegisterType;

@Useless("discovery related")
public class DiscoveryClientImpl extends DiscoveryClient {

    private final String id;
    private final RegisterType type;
    private final String version;
    private final String appName;
    private final int times;
    private final String address;
    private final Map labels;
    private final Consumer registerConsumer;
========
import org.apache.hugegraph.pd.grpc.discovery.NodeInfo;
import org.apache.hugegraph.pd.grpc.discovery.RegisterType;

/**
 * @author zhangyingjie
 * @date 2021/12/20
 **/
public class DiscoveryClientImpl extends DiscoveryClient {

    private volatile String id ;
    private RegisterType type; // 心跳类型，备用
    private String version;
    private String appName;
    private int times; // 心跳过期次数，备用
    private String address;
    private Map labels;
    private Consumer registerConsumer;
    private PDConfig conf;

>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-client/src/main/java/org/apache/hugegraph/pd/client/DiscoveryClientImpl.java

    private DiscoveryClientImpl(Builder builder) {
        super(builder.delay, builder.conf);
        period = builder.delay;
        id = builder.id;
        type = builder.type;
        version = builder.version;
        appName = builder.appName;
        times = builder.times;
        address = builder.address;
        labels = builder.labels;
        registerConsumer = builder.registerConsumer;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    @Override
    NodeInfo getRegisterNode() {
        return NodeInfo.newBuilder().setAddress(this.address)
                       .setVersion(this.version)
                       .setAppName(this.appName).setInterval(this.period)
                       .setId(this.id).putAllLabels(labels).build();
    }

    @Override
    Consumer getRegisterConsumer() {
        return registerConsumer;
    }

<<<<<<<< HEAD:hugegraph-pd/hg-pd-client/src/main/java/org/apache/hugegraph/pd/client/DiscoveryClientImpl.java
========
    @Override
    public void onLeaderChanged(String leaderAddress) {

    }


>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-client/src/main/java/org/apache/hugegraph/pd/client/DiscoveryClientImpl.java
    public static final class Builder {

        private int delay;
        private String centerAddress;
        private String id;
        private RegisterType type;
        private String address;
        private Map labels;
        private String version;
        private String appName;
        private int times;
        private Consumer registerConsumer;
        private PDConfig conf;

        private Builder() {
        }

        public Builder setDelay(int val) {
            delay = val;
            return this;
        }

        public Builder setCenterAddress(String val) {
            centerAddress = val;
            return this;
        }

        public Builder setId(String val) {
            id = val;
            return this;
        }

        public Builder setType(RegisterType val) {
            type = val;
            return this;
        }

        public Builder setAddress(String val) {
            address = val;
            return this;
        }

        public Builder setLabels(Map val) {
            labels = val;
            return this;
        }

        public Builder setVersion(String val) {
            version = val;
            return this;
        }

        public Builder setAppName(String val) {
            appName = val;
            return this;
        }

        public Builder setTimes(int val) {
            times = val;
            return this;
        }

        public Builder setPdConfig(PDConfig val) {
            this.conf = val;
            return this;
        }

        public Builder setRegisterConsumer(Consumer registerConsumer) {
            this.registerConsumer = registerConsumer;
            return this;
        }

        public DiscoveryClientImpl build() {
            return new DiscoveryClientImpl(this);
        }
    }
}
