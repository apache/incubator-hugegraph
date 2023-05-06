/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hugegraph.pd.pulse;

import java.util.function.Function;

import com.baidu.hugegraph.pd.grpc.pulse.*;

/**
 * @author lynn.bond@hotmail.com created on 2021/11/9
 */
public class PartitionHeartbeatSubject extends AbstractObserverSubject {

    PartitionHeartbeatSubject() {
        super(PulseType.PULSE_TYPE_PARTITION_HEARTBEAT);
    }

    @Override
    String toNoticeString(PulseResponse res) {
        return res.getPartitionHeartbeatResponse().toString();
    }

    @Override
    Function<PulseNoticeRequest, PartitionHeartbeatRequest> getNoticeHandler() {
        return r -> r.getPartitionHeartbeatRequest();
    }

    void notifyClient(PartitionHeartbeatResponse.Builder responseBuilder) {

        super.notifyClient(b -> {
            b.setPartitionHeartbeatResponse(responseBuilder);
        });

    }

    long notifyClient(PartitionHeartbeatResponse response) {
        return super.notifyClient(b -> {
            b.setPartitionHeartbeatResponse(response);
        });
    }
}
