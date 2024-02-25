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

package org.apache.hugegraph.pd.pulse;

import java.util.function.Function;

import org.apache.hugegraph.pd.grpc.pulse.PartitionHeartbeatRequest;
import org.apache.hugegraph.pd.grpc.pulse.PartitionHeartbeatResponse;
import org.apache.hugegraph.pd.grpc.pulse.PulseNoticeRequest;
import org.apache.hugegraph.pd.grpc.pulse.PulseResponse;
import org.apache.hugegraph.pd.grpc.pulse.PulseType;

import com.google.protobuf.GeneratedMessageV3;

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

    @Override
    long notifyClient(GeneratedMessageV3 response) {
        return super.notifyClient(b -> {
            b.setPartitionHeartbeatResponse((PartitionHeartbeatResponse) response);
        });
    }
}
