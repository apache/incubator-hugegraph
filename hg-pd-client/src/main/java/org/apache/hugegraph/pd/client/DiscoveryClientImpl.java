package org.apache.hugegraph.pd.client;

import com.baidu.hugegraph.pd.grpc.discovery.NodeInfo;
import com.baidu.hugegraph.pd.grpc.discovery.RegisterType;

import java.util.Map;
import java.util.function.Consumer;

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


    private DiscoveryClientImpl(Builder builder) {
        super(builder.centerAddress, builder.delay);
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

        public Builder setRegisterConsumer(Consumer registerConsumer) {
            this.registerConsumer = registerConsumer;
            return this;
        }

        public DiscoveryClientImpl build() {
            return new DiscoveryClientImpl(this);
        }
    }
}
